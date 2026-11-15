import 'dart:async';
import 'dart:js_interop';
import 'dart:typed_data';
import 'package:web/web.dart' as web;

import 'websocket_transport.dart';

/// Top-level factory function called by [WebSocketNetTransport.connect] on Web platforms.
WebSocketNetTransport createWebSocketClient(
  Uri uri, {
  NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
  WebSocketReconnectPolicy reconnectPolicy = const WebSocketReconnectPolicy(),
  int? peerId,
  Duration pingInterval = const Duration(seconds: 5),
  Duration connectTimeout = const Duration(seconds: 3),
}) {
  return WebSocketClientWeb(
    uri: uri,
    authOptions: authOptions,
    reconnectPolicy: reconnectPolicy,
    peerId: peerId,
    pingInterval: pingInterval,
    connectTimeout: connectTimeout,
  );
}

/// Top-level factory function called by [WebSocketNetTransport.bindServer] on Web platforms.
///
/// Throws [UnsupportedError] as browser environments cannot host TCP/HTTP servers.
Future<WebSocketNetTransport> bindWebSocketServer(
  dynamic host,
  int port, {
  NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
  int? serverPeerId,
  Duration pingInterval = const Duration(seconds: 5),
  dynamic securityContext,
}) {
  throw UnsupportedError('WebSocket server hosting requires dart:io');
}

/// Client-side WebSocket transport for Web platforms (`package:web` + `dart:js_interop`).
class WebSocketClientWeb implements WebSocketNetTransport {
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

  web.WebSocket? _socket;
  Timer? _pingTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  int _sequence = 0;
  final List<NetPacket> _incomingPackets = [];
  Completer<bool>? _connectedCompleter;
  bool _isClosed = false;

  WebSocketClientWeb({
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

  void _startConnect() {
    if (_isClosed) return;
    _state = (_reconnectAttempts == 0)
        ? NetConnectionState.connecting
        : NetConnectionState.reconnecting;
    metrics.connectAttempts++;

    try {
      final targetUri = resolveWebSocketUri(uri, authOptions);
      final ws = web.WebSocket(targetUri.toString());
      ws.binaryType = 'arraybuffer';

      ws.onopen = ((web.Event e) {
        if (_isClosed) {
          ws.close();
          return;
        }
        _socket = ws;
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
      }).toJS;

      ws.onmessage = ((web.MessageEvent e) {
        final d = e.data;
        Uint8List? bytes;
        if (d != null) {
          if (d.isA<JSArrayBuffer>()) {
            bytes = (d as JSArrayBuffer).toDart.asUint8List();
          } else if (d.isA<JSUint8Array>()) {
            bytes = (d as JSUint8Array).toDart;
          }
        }
        if (bytes == null) return;

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
      }).toJS;

      ws.onclose = ((web.CloseEvent e) {
        _handleDisconnectOrError();
      }).toJS;

      ws.onerror = ((web.Event e) {
        _handleDisconnectOrError();
      }).toJS;

      _socket = ws;
    } catch (_) {
      _handleDisconnectOrError();
    }
  }

  void _handleDisconnectOrError() {
    _pingTimer?.cancel();
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
      final ws = _socket;
      if (_state == NetConnectionState.connected && ws != null) {
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

  void _sendRawFrame(WebSocketFrame frame) {
    final ws = _socket;
    if (ws == null || _state != NetConnectionState.connected) return;
    final bytes = frame.encode();
    try {
      ws.send(bytes.toJS);
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
    final ws = _socket;
    _socket = null;
    if (ws != null) {
      try {
        ws.close();
      } catch (_) {}
    }
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
