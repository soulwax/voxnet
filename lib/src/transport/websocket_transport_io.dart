import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'websocket_transport.dart';

/// Top-level factory function called by [WebSocketNetTransport.connect] on IO platforms.
WebSocketNetTransport createWebSocketClient(
  Uri uri, {
  NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
  WebSocketReconnectPolicy reconnectPolicy = const WebSocketReconnectPolicy(),
  int? peerId,
  Duration pingInterval = const Duration(seconds: 5),
  Duration connectTimeout = const Duration(seconds: 3),
}) {
  return WebSocketClientIo(
    uri: uri,
    authOptions: authOptions,
    reconnectPolicy: reconnectPolicy,
    peerId: peerId,
    pingInterval: pingInterval,
    connectTimeout: connectTimeout,
  );
}

/// Top-level factory function called by [WebSocketNetTransport.bindServer] on IO platforms.
Future<WebSocketNetTransport> bindWebSocketServer(
  dynamic host,
  int port, {
  NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
  int? serverPeerId,
  Duration pingInterval = const Duration(seconds: 5),
  dynamic securityContext,
}) async {
  final sId = serverPeerId ?? 0;
  final HttpServer server;
  if (authOptions.useTls && securityContext is SecurityContext) {
    server = await HttpServer.bindSecure(host, port, securityContext);
  } else {
    server = await HttpServer.bind(host, port);
  }

  return WebSocketServerIo._(
    server: server,
    authOptions: authOptions,
    serverPeerId: sId,
    pingInterval: pingInterval,
  );
}

/// Client-side WebSocket transport for IO platforms (`dart:io`).
class WebSocketClientIo implements WebSocketNetTransport {
  @override
  final Uri uri;

  @override
  final NetTransportAuthOptions authOptions;

  @override
  final WebSocketReconnectPolicy reconnectPolicy;

  final Duration pingInterval;
  final Duration connectTimeout;

  @override
  final NetTransportMetrics metrics = NetTransportMetrics();

  NetConnectionState _state = NetConnectionState.disconnected;
  @override
  NetConnectionState get connectionState => _state;

  int _peerId = 0;
  @override
  int get peerId => _peerId;

  @override
  int get port => uri.hasPort ? uri.port : (uri.scheme == 'wss' ? 443 : 80);

  int _serverPeerId = 0;
  @override
  List<int> get connectedPeerIds =>
      (_state == NetConnectionState.connected) ? [_serverPeerId] : const [];

  WebSocket? _socket;
  StreamSubscription? _subscription;
  Timer? _pingTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  int _sequence = 0;
  final List<NetPacket> _incomingPackets = [];
  Completer<bool>? _connectedCompleter;
  bool _isClosed = false;

  WebSocketClientIo({
    required this.uri,
    this.authOptions = const NetTransportAuthOptions(),
    this.reconnectPolicy = const WebSocketReconnectPolicy(),
    int? peerId,
    this.pingInterval = const Duration(seconds: 5),
    this.connectTimeout = const Duration(seconds: 3),
  }) : _peerId = peerId ?? 0 {
    _connectedCompleter = Completer<bool>();
    _startConnect();
  }

  void _startConnect() async {
    if (_isClosed) return;
    _state = (_reconnectAttempts == 0)
        ? NetConnectionState.connecting
        : NetConnectionState.reconnecting;
    metrics.connectAttempts++;

    try {
      final headers = authOptions.resolvedHeaders();
      SecurityContext? secContext;
      if (authOptions.tlsCertificatePath != null) {
        secContext = SecurityContext()
          ..setTrustedCertificates(authOptions.tlsCertificatePath!);
      }
      final customClient = HttpClient(context: secContext)
        ..connectionTimeout = connectTimeout;
      if (authOptions.allowInsecureLocalhostTls) {
        customClient.badCertificateCallback = (cert, host, port) => true;
      }

      final socket = await WebSocket.connect(
        uri.toString(),
        headers: headers.isEmpty ? null : headers,
        customClient: customClient,
      );

      if (_isClosed) {
        await socket.close();
        return;
      }

      _socket = socket;
      _state = NetConnectionState.connected;
      _reconnectAttempts = 0;
      if (_connectedCompleter != null && !_connectedCompleter!.isCompleted) {
        _connectedCompleter!.complete(true);
      }

      if (_peerId > 0) {
        _sendRawFrame(
          WebSocketFrame(
            type: WebSocketFrame.typeHandshake,
            senderId: _peerId,
            recipientId: 0,
            timestampMs: DateTime.now().millisecondsSinceEpoch,
            payload: Uint8List(0),
          ),
        );
      }

      _startPingTimer();

      _subscription = socket.listen(
        _onMessage,
        onError: (_) => _handleDisconnectOrError(),
        onDone: () => _handleDisconnectOrError(),
        cancelOnError: true,
      );
    } catch (_) {
      if (_isClosed) return;
      _handleDisconnectOrError();
    }
  }

  void _handleDisconnectOrError() {
    _pingTimer?.cancel();
    _subscription?.cancel();
    _subscription = null;
    _socket = null;

    if (_isClosed) {
      _state = NetConnectionState.disconnected;
      return;
    }

    _reconnectAttempts++;
    if (reconnectPolicy.canRetry(_reconnectAttempts)) {
      _state = NetConnectionState.reconnecting;
      metrics.reconnectCount++;
      if (_connectedCompleter == null || _connectedCompleter!.isCompleted) {
        _connectedCompleter = Completer<bool>();
      }
      final delayMs = reconnectPolicy.delayForAttempt(_reconnectAttempts);
      _reconnectTimer = Timer(Duration(milliseconds: delayMs), _startConnect);
    } else {
      _state = NetConnectionState.failed;
      if (_connectedCompleter != null && !_connectedCompleter!.isCompleted) {
        _connectedCompleter!.complete(false);
      }
    }
  }

  void _startPingTimer() {
    _pingTimer?.cancel();
    if (pingInterval <= Duration.zero) return;
    _pingTimer = Timer.periodic(pingInterval, (_) {
      if (_state == NetConnectionState.connected && _socket != null) {
        final now = DateTime.now().millisecondsSinceEpoch;
        _sendRawFrame(
          WebSocketFrame(
            type: WebSocketFrame.typePing,
            senderId: _peerId,
            recipientId: _serverPeerId,
            sequence: ++_sequence,
            timestampMs: now,
            payload: Uint8List(0),
          ),
        );
      }
    });
  }

  void _onMessage(dynamic raw) {
    Uint8List bytes;
    if (raw is Uint8List) {
      bytes = raw;
    } else if (raw is List<int>) {
      bytes = Uint8List.fromList(raw);
    } else {
      return;
    }

    metrics.recordReceive(bytes.length);

    final frame = WebSocketFrame.decode(bytes);
    if (frame == null) {
      metrics.recordDrop();
      return;
    }

    switch (frame.type) {
      case WebSocketFrame.typePing:
        _sendRawFrame(
          WebSocketFrame(
            type: WebSocketFrame.typePong,
            channel: frame.channel,
            senderId: _peerId,
            recipientId: frame.senderId,
            sequence: frame.sequence,
            timestampMs: frame.timestampMs,
            payload: Uint8List(0),
          ),
        );
        break;

      case WebSocketFrame.typePong:
        final now = DateTime.now().millisecondsSinceEpoch;
        final rtt = (now - frame.timestampMs).toDouble();
        metrics.recordRtt(rtt);
        break;

      case WebSocketFrame.typeHandshake:
        _serverPeerId = frame.senderId;
        if (_peerId <= 0) {
          _peerId = frame.recipientId;
        }
        break;

      case WebSocketFrame.typeData:
        _incomingPackets.add(
          NetPacket(
            senderId: frame.senderId,
            data: frame.payload,
            timestampMs: frame.timestampMs,
            channel: frame.channel,
          ),
        );
        break;
    }
  }

  void _sendRawFrame(WebSocketFrame frame) {
    final socket = _socket;
    if (socket == null || _state != NetConnectionState.connected) return;
    final bytes = frame.encode();
    try {
      socket.add(bytes);
      metrics.recordSend(bytes.length);
    } catch (_) {
      metrics.recordDrop();
    }
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
    if (_state != NetConnectionState.connected) return;
    final frame = WebSocketFrame(
      type: WebSocketFrame.typeData,
      channel: channel,
      senderId: _peerId,
      recipientId: recipientId,
      sequence: ++_sequence,
      timestampMs: DateTime.now().millisecondsSinceEpoch,
      payload: data,
    );
    _sendRawFrame(frame);
  }

  @override
  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    sendWithChannel(-1, data, channel: channel);
  }

  @override
  List<NetPacket> poll([int? currentTimestampMs]) {
    if (_incomingPackets.isEmpty) return const [];
    final ready = List<NetPacket>.of(_incomingPackets);
    _incomingPackets.clear();
    return ready;
  }

  @override
  void close() {
    closeAsync();
  }

  @override
  Future<void> closeAsync() async {
    _isClosed = true;
    _state = NetConnectionState.closing;
    _pingTimer?.cancel();
    _reconnectTimer?.cancel();
    await _subscription?.cancel();
    _subscription = null;
    try {
      await _socket?.close();
    } catch (_) {}
    _socket = null;
    _state = NetConnectionState.disconnected;
  }

  @override
  Future<bool> waitUntilConnected({
    Duration timeout = const Duration(seconds: 5),
  }) async {
    if (_state == NetConnectionState.connected) return true;
    if (_state == NetConnectionState.disconnected ||
        _state == NetConnectionState.failed) {
      return false;
    }
    final completer = _connectedCompleter;
    if (completer == null) return false;
    try {
      return await completer.future.timeout(timeout);
    } catch (_) {
      return _state == NetConnectionState.connected;
    }
  }
}

class _ClientSessionIo {
  final int peerId;
  final WebSocket socket;
  final StreamSubscription subscription;

  _ClientSessionIo({
    required this.peerId,
    required this.socket,
    required this.subscription,
  });
}

/// Server-side WebSocket transport for IO platforms (`dart:io`).
class WebSocketServerIo implements WebSocketNetTransport {
  final HttpServer _httpServer;
  final int _serverPeerId;

  @override
  int get port => _httpServer.port;

  @override
  int get peerId => _serverPeerId;

  @override
  Uri? get uri => null;

  @override
  final NetTransportAuthOptions authOptions;

  @override
  final WebSocketReconnectPolicy reconnectPolicy =
      const WebSocketReconnectPolicy();

  final Duration pingInterval;

  @override
  final NetTransportMetrics metrics = NetTransportMetrics();

  NetConnectionState _state = NetConnectionState.connected;
  @override
  NetConnectionState get connectionState => _state;

  final Map<int, _ClientSessionIo> _clients = {};
  @override
  List<int> get connectedPeerIds => List<int>.unmodifiable(_clients.keys);

  final List<NetPacket> _incomingPackets = [];
  int _nextPeerId = 1;
  int _sequence = 0;
  Timer? _pingTimer;
  bool _isClosed = false;

  WebSocketServerIo._({
    required HttpServer server,
    required this.authOptions,
    required this._serverPeerId,
    required this.pingInterval,
  }) : _httpServer = server {
    _httpServer.listen(_handleHttpRequest);
    _startPingTimer();
  }

  void _startPingTimer() {
    if (pingInterval <= Duration.zero) return;
    _pingTimer = Timer.periodic(pingInterval, (_) {
      if (_isClosed) return;
      final now = DateTime.now().millisecondsSinceEpoch;
      for (final client in _clients.values) {
        _sendToClient(
          client.peerId,
          WebSocketFrame(
            type: WebSocketFrame.typePing,
            senderId: _serverPeerId,
            recipientId: client.peerId,
            sequence: ++_sequence,
            timestampMs: now,
            payload: Uint8List(0),
          ),
        );
      }
    });
  }

  void _handleHttpRequest(HttpRequest request) async {
    if (_isClosed) {
      request.response.statusCode = HttpStatus.serviceUnavailable;
      await request.response.close();
      return;
    }

    if (authOptions.bearerToken != null &&
        authOptions.bearerToken!.isNotEmpty) {
      final expectedToken = authOptions.bearerToken!;
      final authHeader = request.headers.value('authorization');
      final queryToken =
          request.uri.queryParameters['token'] ??
          request.uri.queryParameters['bearer_token'];
      final protocols = request.headers.value('sec-websocket-protocol');

      bool authorized = false;
      if (authHeader != null) {
        if (authHeader.startsWith('Bearer ')) {
          authorized = (authHeader.substring(7).trim() == expectedToken);
        } else if (authHeader == expectedToken) {
          authorized = true;
        }
      }
      if (!authorized && queryToken != null && queryToken == expectedToken) {
        authorized = true;
      }
      if (!authorized &&
          protocols != null &&
          protocols.contains(expectedToken)) {
        authorized = true;
      }

      if (!authorized) {
        metrics.authFailures++;
        request.response.statusCode = HttpStatus.unauthorized;
        request.response.headers.set('WWW-Authenticate', 'Bearer');
        await request.response.close();
        return;
      }
    }

    for (final entry in authOptions.headers.entries) {
      if (entry.key.toLowerCase() == 'authorization') continue;
      final actual = request.headers.value(entry.key);
      if (actual != entry.value) {
        metrics.authFailures++;
        request.response.statusCode = HttpStatus.unauthorized;
        await request.response.close();
        return;
      }
    }

    try {
      final socket = await WebSocketTransformer.upgrade(request);
      if (_isClosed) {
        await socket.close();
        return;
      }
      _registerClient(socket);
    } catch (_) {
      try {
        request.response.statusCode = HttpStatus.badRequest;
        await request.response.close();
      } catch (_) {}
    }
  }

  void _registerClient(WebSocket socket) {
    final assignedPeerId = _nextPeerId++;
    final sub = socket.listen(
      (data) => _onClientMessage(assignedPeerId, data),
      onError: (_) => _removeClient(assignedPeerId),
      onDone: () => _removeClient(assignedPeerId),
      cancelOnError: true,
    );
    final session = _ClientSessionIo(
      peerId: assignedPeerId,
      socket: socket,
      subscription: sub,
    );
    _clients[assignedPeerId] = session;

    final welcomeFrame = WebSocketFrame(
      type: WebSocketFrame.typeHandshake,
      senderId: _serverPeerId,
      recipientId: assignedPeerId,
      timestampMs: DateTime.now().millisecondsSinceEpoch,
      payload: Uint8List(0),
    );
    final bytes = welcomeFrame.encode();
    try {
      socket.add(bytes);
      metrics.recordSend(bytes.length);
    } catch (_) {
      metrics.recordDrop();
    }
  }

  void _removeClient(int peerId) {
    final session = _clients.remove(peerId);
    if (session != null) {
      session.subscription.cancel();
      try {
        session.socket.close();
      } catch (_) {}
    }
  }

  void _onClientMessage(int clientPeerId, dynamic raw) {
    Uint8List bytes;
    if (raw is Uint8List) {
      bytes = raw;
    } else if (raw is List<int>) {
      bytes = Uint8List.fromList(raw);
    } else {
      return;
    }

    metrics.recordReceive(bytes.length);

    final frame = WebSocketFrame.decode(bytes);
    if (frame == null) {
      metrics.recordDrop();
      return;
    }

    switch (frame.type) {
      case WebSocketFrame.typePing:
        _sendToClient(
          clientPeerId,
          WebSocketFrame(
            type: WebSocketFrame.typePong,
            channel: frame.channel,
            senderId: _serverPeerId,
            recipientId: clientPeerId,
            sequence: frame.sequence,
            timestampMs: frame.timestampMs,
            payload: Uint8List(0),
          ),
        );
        break;

      case WebSocketFrame.typePong:
        final now = DateTime.now().millisecondsSinceEpoch;
        final rtt = (now - frame.timestampMs).toDouble();
        metrics.recordRtt(rtt);
        break;

      case WebSocketFrame.typeHandshake:
        if (frame.senderId > 0 &&
            frame.senderId != clientPeerId &&
            !_clients.containsKey(frame.senderId)) {
          final session = _clients.remove(clientPeerId);
          if (session != null) {
            _clients[frame.senderId] = _ClientSessionIo(
              peerId: frame.senderId,
              socket: session.socket,
              subscription: session.subscription,
            );
          }
        }
        break;

      case WebSocketFrame.typeData:
        if (frame.recipientId == _serverPeerId || frame.recipientId == 0) {
          _incomingPackets.add(
            NetPacket(
              senderId: frame.senderId,
              data: frame.payload,
              timestampMs: frame.timestampMs,
              channel: frame.channel,
            ),
          );
        } else if (frame.recipientId == -1) {
          _incomingPackets.add(
            NetPacket(
              senderId: frame.senderId,
              data: frame.payload,
              timestampMs: frame.timestampMs,
              channel: frame.channel,
            ),
          );
          for (final client in _clients.values) {
            if (client.peerId != frame.senderId) {
              _sendToClient(client.peerId, frame);
            }
          }
        } else {
          _sendToClient(frame.recipientId, frame);
        }
        break;
    }
  }

  void _sendToClient(int recipientId, WebSocketFrame frame) {
    final client = _clients[recipientId];
    if (client == null) {
      metrics.recordDrop();
      return;
    }
    final bytes = frame.encode();
    try {
      client.socket.add(bytes);
      metrics.recordSend(bytes.length);
    } catch (_) {
      metrics.recordDrop();
    }
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
    final frame = WebSocketFrame(
      type: WebSocketFrame.typeData,
      channel: channel,
      senderId: _serverPeerId,
      recipientId: recipientId,
      sequence: ++_sequence,
      timestampMs: DateTime.now().millisecondsSinceEpoch,
      payload: data,
    );
    _sendToClient(recipientId, frame);
  }

  @override
  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    final frame = WebSocketFrame(
      type: WebSocketFrame.typeData,
      channel: channel,
      senderId: _serverPeerId,
      recipientId: -1,
      sequence: ++_sequence,
      timestampMs: DateTime.now().millisecondsSinceEpoch,
      payload: data,
    );
    for (final peerId in _clients.keys) {
      _sendToClient(peerId, frame);
    }
  }

  @override
  List<NetPacket> poll([int? currentTimestampMs]) {
    if (_incomingPackets.isEmpty) return const [];
    final ready = List<NetPacket>.of(_incomingPackets);
    _incomingPackets.clear();
    return ready;
  }

  @override
  void close() {
    closeAsync();
  }

  @override
  Future<void> closeAsync() async {
    if (_isClosed) return;
    _isClosed = true;
    _state = NetConnectionState.closing;
    _pingTimer?.cancel();
    for (final client in _clients.values) {
      try {
        await client.subscription.cancel();
        await client.socket.close();
      } catch (_) {}
    }
    _clients.clear();
    try {
      await _httpServer.close(force: true);
    } catch (_) {}
    _state = NetConnectionState.disconnected;
  }

  @override
  Future<bool> waitUntilConnected({
    Duration timeout = const Duration(seconds: 5),
  }) async {
    return !_isClosed;
  }
}
