import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'reliable_channel.dart';
import 'transport.dart';

class _UdpPeerSession {
  final int peerId;
  final InternetAddress address;
  final int port;
  final ReliableChannelEndpoint channel;

  _UdpPeerSession({
    required this.peerId,
    required this.address,
    required this.port,
    required this.channel,
  });
}

class _RawDatagramTarget {
  final InternetAddress address;
  final int port;

  const _RawDatagramTarget(this.address, this.port);
}

/// Native `dart:io` [RawDatagramSocket] implementation of [NetTransport]
/// backed by per-peer [ReliableChannelEndpoint] reliability, MTU
/// fragmentation, AIMD congestion windowing, and optional [UdpFaultInjector].
class UdpNetTransport implements NetTransport {
  final RawDatagramSocket _socket;
  final bool _isServer;
  int _localPeerId;
  @override
  final NetTransportAuthOptions authOptions;
  @override
  final NetTransportMetrics metrics = NetTransportMetrics();

  final int maxMtuBytes;
  final UdpFaultInjector? faultInjector;
  late final UdpFaultQueue<_RawDatagramTarget>? _faultQueue =
      faultInjector != null
      ? UdpFaultQueue<_RawDatagramTarget>(faultInjector!)
      : null;

  NetConnectionState _connectionState;
  bool _closed = false;
  int _nextAssignedPeerId = 2;
  final int _handshakeSynSentAtMs;
  int? _serverPeerId;

  final Map<int, _UdpPeerSession> _peersById = {};
  final Map<String, _UdpPeerSession> _peersByEndpoint = {};
  final Queue<Datagram> _rawInboundQueue = Queue<Datagram>();
  final List<NetPacket> _deliveredPackets = [];
  StreamSubscription<RawSocketEvent>? _subscription;

  UdpNetTransport._({
    required RawDatagramSocket socket,
    required bool isServer,
    required int localPeerId,
    required this.authOptions,
    required this.maxMtuBytes,
    required this.faultInjector,
    required NetConnectionState initialState,
    int? serverPeerId,
  }) : _socket = socket,
       _isServer = isServer,
       _localPeerId = localPeerId,
       _connectionState = initialState,
       _handshakeSynSentAtMs = DateTime.now().millisecondsSinceEpoch,
       _serverPeerId = serverPeerId {
    _subscription = _socket.listen((event) {
      if (event == RawSocketEvent.read) {
        _drainSocket();
      }
    });
  }

  /// Binds a UDP server socket on [host] and [port].
  static Future<UdpNetTransport> bind(
    dynamic host,
    int port, {
    int localPeerId = 0,
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    UdpFaultInjector? faultInjector,
    int maxMtuBytes = 1200,
  }) async {
    final addr = await _resolveAddress(host);
    final socket = await RawDatagramSocket.bind(addr, port);
    final transport = UdpNetTransport._(
      socket: socket,
      isServer: true,
      localPeerId: localPeerId,
      authOptions: authOptions,
      maxMtuBytes: maxMtuBytes,
      faultInjector: faultInjector,
      initialState: NetConnectionState.connected,
    );
    return transport;
  }

  /// Binds an ephemeral local UDP socket, registers the server peer at
  /// [host]:[remotePort], and dispatches an authenticated handshake packet.
  static Future<UdpNetTransport> connect(
    dynamic host,
    int remotePort, {
    int localPeerId = 1,
    int remotePeerId = 0,
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    UdpFaultInjector? faultInjector,
    int maxMtuBytes = 1200,
    dynamic bindHost,
  }) async {
    final remoteAddr = await _resolveAddress(host);
    final localAddr = bindHost != null
        ? await _resolveAddress(bindHost)
        : (remoteAddr.type == InternetAddressType.IPv6
              ? InternetAddress.anyIPv6
              : InternetAddress.loopbackIPv4);

    final socket = await RawDatagramSocket.bind(localAddr, 0);
    final transport = UdpNetTransport._(
      socket: socket,
      isServer: false,
      localPeerId: localPeerId,
      authOptions: authOptions,
      maxMtuBytes: maxMtuBytes,
      faultInjector: faultInjector,
      initialState: NetConnectionState.connecting,
      serverPeerId: remotePeerId,
    );
    transport.metrics.connectAttempts++;
    transport.addPeer(remotePeerId, remoteAddr, remotePort);
    transport._sendHandshakeSyn(remoteAddr, remotePort);
    return transport;
  }

  int get port => _socket.port;
  int get peerId => _localPeerId;
  bool get isServer => _isServer;

  @override
  NetConnectionState get connectionState => _connectionState;

  @override
  List<int> get connectedPeerIds =>
      List<int>.unmodifiable(_peersById.keys.toList()..sort());

  /// Manually registers a peer endpoint `(address, remotePort)` under [peerId].
  void addPeer(int peerId, InternetAddress address, int remotePort) {
    final ep = _UdpPeerSession(
      peerId: peerId,
      address: address,
      port: remotePort,
      channel: ReliableChannelEndpoint(
        localPeerId: _localPeerId,
        remotePeerId: peerId,
        maxMtuBytes: maxMtuBytes,
        metrics: metrics,
      ),
    );
    _peersById[peerId] = ep;
    _peersByEndpoint[_endpointKey(address, remotePort)] = ep;
  }

  /// Waits up to [timeout] while pumping [poll] until handshake completes.
  Future<bool> waitUntilConnected({
    Duration timeout = const Duration(seconds: 5),
  }) async {
    final deadline = DateTime.now().add(timeout);
    while (!_closed && DateTime.now().isBefore(deadline)) {
      poll();
      if (_connectionState == NetConnectionState.connected) {
        return true;
      }
      if (_connectionState == NetConnectionState.failed ||
          _connectionState == NetConnectionState.disconnected) {
        return false;
      }
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    return _connectionState == NetConnectionState.connected;
  }

  @override
  void send(int recipientId, Uint8List data) {
    sendWithChannel(
      recipientId,
      data,
      channel: NetDeliveryChannel.reliableOrdered,
    );
  }

  @override
  void broadcast(Uint8List data) {
    broadcastWithChannel(data, channel: NetDeliveryChannel.reliableOrdered);
  }

  @override
  void sendWithChannel(
    int recipientId,
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    if (_closed) return;
    final peer = _peersById[recipientId];
    if (peer == null) return;
    peer.channel.queueSend(
      data,
      channel: channel,
      nowMs: DateTime.now().millisecondsSinceEpoch,
    );
  }

  @override
  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    if (_closed) return;
    final nowMs = DateTime.now().millisecondsSinceEpoch;
    for (final peer in _peersById.values) {
      peer.channel.queueSend(data, channel: channel, nowMs: nowMs);
    }
  }

  @override
  List<NetPacket> poll([int? currentTimestampMs]) {
    if (_closed) return const [];
    final nowMs = currentTimestampMs ?? DateTime.now().millisecondsSinceEpoch;

    _drainSocket();

    // Flush delayed synthetic-fault datagrams whose delivery time elapsed.
    final faultQ = _faultQueue;
    if (faultQ != null) {
      for (final ready in faultQ.drainReady(nowMs)) {
        _sendRawNow(ready.target.address, ready.target.port, ready.payload);
      }
    }

    // Flush scheduled channel packets and RTO retransmits per peer.
    for (final peer in _peersById.values) {
      final datagrams = peer.channel.collectOutgoingDatagrams(nowMs);
      for (final dg in datagrams) {
        _dispatchDatagram(peer.address, peer.port, dg, nowMs);
      }
    }

    // Read inbound datagrams queued from RawDatagramSocket.
    while (_rawInboundQueue.isNotEmpty) {
      final dg = _rawInboundQueue.removeFirst();
      _handleInboundDatagram(dg, nowMs);
    }

    // Also emit ACKs generated immediately in response to inbound traffic.
    for (final peer in _peersById.values) {
      final immediateAcks = peer.channel.collectOutgoingDatagrams(nowMs);
      for (final dg in immediateAcks) {
        _dispatchDatagram(peer.address, peer.port, dg, nowMs);
      }
    }

    if (_deliveredPackets.isEmpty) return const [];
    final result = List<NetPacket>.from(_deliveredPackets);
    _deliveredPackets.clear();
    return result;
  }

  void _drainSocket() {
    if (_closed) return;
    while (true) {
      final dg = _socket.receive();
      if (dg == null) break;
      _rawInboundQueue.addLast(dg);
    }
  }

  void _handleInboundDatagram(Datagram dg, int nowMs) {
    final frame = ReliableChannelFrame.decode(dg.data);
    if (frame == null) return;
    metrics.recordReceive(dg.data.length);

    final key = _endpointKey(dg.address, dg.port);
    if (frame.type == ReliableChannelFrame.typeHandshakeSyn && _isServer) {
      final token = frame.payload.isEmpty ? '' : utf8.decode(frame.payload);
      final expectedToken = authOptions.bearerToken;
      if (expectedToken != null &&
          expectedToken.isNotEmpty &&
          token != expectedToken) {
        metrics.authFailures++;
        return;
      }

      var peer = _peersByEndpoint[key];
      if (peer == null) {
        final assignedId =
            frame.senderId > 0 && !_peersById.containsKey(frame.senderId)
            ? frame.senderId
            : _allocatePeerId();
        peer = _UdpPeerSession(
          peerId: assignedId,
          address: dg.address,
          port: dg.port,
          channel: ReliableChannelEndpoint(
            localPeerId: _localPeerId,
            remotePeerId: assignedId,
            maxMtuBytes: maxMtuBytes,
            metrics: metrics,
          ),
        );
        _peersById[assignedId] = peer;
        _peersByEndpoint[key] = peer;
      }

      final ackPayload = Uint8List(4);
      ByteData.sublistView(ackPayload).setInt32(0, peer.peerId, Endian.little);
      final ackFrame = ReliableChannelFrame(
        type: ReliableChannelFrame.typeHandshakeAck,
        channel: NetDeliveryChannel.reliableOrdered,
        senderId: _localPeerId,
        sequence: 0,
        ack: 0,
        ackBits: 0,
        messageSeq: 0,
        fragmentIndex: 0,
        fragmentCount: 1,
        payload: ackPayload,
      ).encode();
      _sendRawNow(dg.address, dg.port, ackFrame);
      return;
    }

    if (frame.type == ReliableChannelFrame.typeHandshakeAck && !_isServer) {
      if (frame.payload.length >= 4) {
        _localPeerId = ByteData.sublistView(
          frame.payload,
        ).getInt32(0, Endian.little);
      }
      metrics.recordRtt(
        (nowMs - _handshakeSynSentAtMs).toDouble().clamp(0.0, 60000.0),
      );
      _connectionState = NetConnectionState.connected;
      return;
    }

    if (frame.type == ReliableChannelFrame.typeDisconnect) {
      final existing = _peersByEndpoint.remove(key);
      if (existing != null) {
        _peersById.remove(existing.peerId);
      }
      if (!_isServer) {
        _connectionState = NetConnectionState.disconnected;
      }
      return;
    }

    var peer = _peersByEndpoint[key];
    if (peer == null && _isServer && authOptions.bearerToken == null) {
      // Allow direct addPeer/implicit session when no bearer token is required.
      final assignedId =
          frame.senderId > 0 && !_peersById.containsKey(frame.senderId)
          ? frame.senderId
          : _allocatePeerId();
      peer = _UdpPeerSession(
        peerId: assignedId,
        address: dg.address,
        port: dg.port,
        channel: ReliableChannelEndpoint(
          localPeerId: _localPeerId,
          remotePeerId: assignedId,
          maxMtuBytes: maxMtuBytes,
          metrics: metrics,
        ),
      );
      _peersById[assignedId] = peer;
      _peersByEndpoint[key] = peer;
    } else if (peer == null && !_isServer && _serverPeerId != null) {
      peer = _peersById[_serverPeerId];
    }

    if (peer == null) return;
    final delivered = peer.channel.receiveFrame(frame, nowMs);
    if (delivered.isNotEmpty) {
      _deliveredPackets.addAll(delivered);
    }
  }

  void _sendHandshakeSyn(InternetAddress address, int remotePort) {
    final tokenBytes = authOptions.bearerToken == null
        ? Uint8List(0)
        : Uint8List.fromList(utf8.encode(authOptions.bearerToken!));
    final syn = ReliableChannelFrame(
      type: ReliableChannelFrame.typeHandshakeSyn,
      channel: NetDeliveryChannel.reliableOrdered,
      senderId: _localPeerId,
      sequence: 0,
      ack: 0,
      ackBits: 0,
      messageSeq: 0,
      fragmentIndex: 0,
      fragmentCount: 1,
      payload: tokenBytes,
    ).encode();
    _sendRawNow(address, remotePort, syn);
  }

  void _dispatchDatagram(
    InternetAddress addr,
    int remotePort,
    Uint8List payload,
    int nowMs,
  ) {
    final faultQ = _faultQueue;
    if (faultQ != null && !faultQ.injector.isPassThrough) {
      metrics.recordSend(payload.length);
      faultQ.enqueue(
        _RawDatagramTarget(addr, remotePort),
        payload,
        nowMs,
        metrics: metrics,
      );
      return;
    }
    metrics.recordSend(payload.length);
    _sendRawNow(addr, remotePort, payload);
  }

  void _sendRawNow(InternetAddress addr, int remotePort, Uint8List payload) {
    if (_closed) return;
    _socket.send(payload, addr, remotePort);
  }

  int _allocatePeerId() {
    while (_peersById.containsKey(_nextAssignedPeerId)) {
      _nextAssignedPeerId++;
    }
    return _nextAssignedPeerId++;
  }

  @override
  void close() {
    if (_closed) return;
    _closed = true;
    _connectionState = NetConnectionState.disconnected;
    _subscription?.cancel();
    _subscription = null;
    _socket.close();
  }

  Future<void> closeAsync() async {
    final sub = _subscription;
    _subscription = null;
    if (!_closed) {
      _closed = true;
      _connectionState = NetConnectionState.disconnected;
      if (sub != null) {
        await sub.cancel();
      }
      _socket.close();
    }
  }

  static Future<InternetAddress> _resolveAddress(dynamic host) async {
    if (host is InternetAddress) return host;
    final str = host.toString();
    final parsed = InternetAddress.tryParse(str);
    if (parsed != null) return parsed;
    final list = await InternetAddress.lookup(str);
    return list.first;
  }

  static String _endpointKey(InternetAddress address, int port) =>
      '${address.address}:$port';
}
