import 'dart:math' as math;
import 'dart:typed_data';

import 'transport.dart';
import 'websocket_transport_io.dart'
    if (dart.library.js_interop) 'websocket_transport_web.dart'
    as impl;

export 'transport.dart'
    show
        NetConnectionState,
        NetDeliveryChannel,
        NetTransportAuthOptions,
        NetTransportMetrics,
        NetTransportMetricsSnapshot,
        NetPacket,
        NetTransport;

/// Configuration policy for exponential reconnect backoff in WebSocket transports.
class WebSocketReconnectPolicy {
  final int initialDelayMs;
  final int maxDelayMs;
  final double backoffFactor;
  final int maxAttempts;

  const WebSocketReconnectPolicy({
    this.initialDelayMs = 500,
    this.maxDelayMs = 10000,
    this.backoffFactor = 2.0,
    this.maxAttempts = 5,
  });

  /// Whether another reconnect attempt is permitted given [attempt] (1-based).
  bool canRetry(int attempt) {
    if (maxAttempts <= 0) return true;
    return attempt <= maxAttempts;
  }

  /// Calculates the backoff delay in milliseconds for [attempt] (1-based).
  int delayForAttempt(int attempt) {
    if (attempt <= 1) return initialDelayMs;
    final factor = math.pow(backoffFactor, attempt - 1);
    final delay = (initialDelayMs * factor).round();
    return math.min(delay, maxDelayMs);
  }

  /// Calculates the [Duration] for [attempt].
  Duration delayDuration(int attempt) =>
      Duration(milliseconds: delayForAttempt(attempt));

  WebSocketReconnectPolicy copyWith({
    int? initialDelayMs,
    int? maxDelayMs,
    double? backoffFactor,
    int? maxAttempts,
  }) {
    return WebSocketReconnectPolicy(
      initialDelayMs: initialDelayMs ?? this.initialDelayMs,
      maxDelayMs: maxDelayMs ?? this.maxDelayMs,
      backoffFactor: backoffFactor ?? this.backoffFactor,
      maxAttempts: maxAttempts ?? this.maxAttempts,
    );
  }
}

/// Binary framing envelope for multiplexed channels, keepalive ping/pong, and metadata.
///
/// Envelope layout (22 bytes header + payload):
/// - byte 0: Frame Type (1 = Data, 2 = Ping, 3 = Pong, 4 = Handshake)
/// - byte 1: NetDeliveryChannel index (0..3)
/// - bytes 2..5: Sender ID (int32, big-endian)
/// - bytes 6..9: Recipient ID (int32, big-endian; -1 indicates broadcast)
/// - bytes 10..13: Sequence number (uint32, big-endian)
/// - bytes 14..21: Timestamp in milliseconds (int64, big-endian)
/// - bytes 22..end: Binary payload
class WebSocketFrame {
  static const int typeData = 1;
  static const int typePing = 2;
  static const int typePong = 3;
  static const int typeHandshake = 4;
  static const int headerSize = 22;

  final int type;
  final NetDeliveryChannel channel;
  final int senderId;
  final int recipientId;
  final int sequence;
  final int timestampMs;
  final Uint8List payload;

  const WebSocketFrame({
    required this.type,
    this.channel = NetDeliveryChannel.reliableOrdered,
    required this.senderId,
    this.recipientId = 0,
    this.sequence = 0,
    required this.timestampMs,
    required this.payload,
  });

  /// Encodes this frame into a [Uint8List] buffer.
  Uint8List encode() {
    final bytes = Uint8List(headerSize + payload.length);
    final bd = ByteData.sublistView(bytes);
    bd.setUint8(0, type);
    bd.setUint8(1, channel.index);
    bd.setInt32(2, senderId);
    bd.setInt32(6, recipientId);
    bd.setUint32(10, sequence);
    bd.setInt64(14, timestampMs);
    if (payload.isNotEmpty) {
      bytes.setRange(headerSize, headerSize + payload.length, payload);
    }
    return bytes;
  }

  /// Decodes a [WebSocketFrame] from [bytes], or returns `null` if the buffer is malformed.
  static WebSocketFrame? decode(Uint8List bytes) {
    if (bytes.length < headerSize) return null;
    final bd = ByteData.sublistView(bytes);
    final type = bd.getUint8(0);
    final channelIdx = bd.getUint8(1);
    final channel =
        (channelIdx >= 0 && channelIdx < NetDeliveryChannel.values.length)
        ? NetDeliveryChannel.values[channelIdx]
        : NetDeliveryChannel.reliableOrdered;
    final senderId = bd.getInt32(2);
    final recipientId = bd.getInt32(6);
    final sequence = bd.getUint32(10);
    final timestampMs = bd.getInt64(14);
    final payload = Uint8List.sublistView(bytes, headerSize);
    return WebSocketFrame(
      type: type,
      channel: channel,
      senderId: senderId,
      recipientId: recipientId,
      sequence: sequence,
      timestampMs: timestampMs,
      payload: payload,
    );
  }
}

/// Resolves a WebSocket [Uri] for Web environments where arbitrary HTTP headers
/// cannot be set, appending the `token` query parameter if [authOptions.bearerToken] is present.
Uri resolveWebSocketUri(Uri uri, NetTransportAuthOptions authOptions) {
  final token = authOptions.bearerToken;
  if (token != null && token.isNotEmpty) {
    if (!uri.queryParameters.containsKey('token') &&
        !uri.queryParameters.containsKey('bearer_token')) {
      final q = Map<String, String>.from(uri.queryParameters);
      q['token'] = token;
      return uri.replace(queryParameters: q);
    }
  }
  return uri;
}

/// Cross-platform WebSocket-based implementation of [NetTransport].
///
/// Supports client connection across VM and Web via conditional imports, and
/// WebSocket server hosting on VM (`dart:io`).
abstract class WebSocketNetTransport implements NetTransport {
  /// Connects to a remote WebSocket server at [uri] as a client.
  factory WebSocketNetTransport.connect(
    Uri uri, {
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    WebSocketReconnectPolicy reconnectPolicy = const WebSocketReconnectPolicy(),
    int? peerId,
    Duration pingInterval = const Duration(seconds: 5),
    Duration connectTimeout = const Duration(seconds: 3),
  }) {
    return impl.createWebSocketClient(
      uri,
      authOptions: authOptions,
      reconnectPolicy: reconnectPolicy,
      peerId: peerId,
      pingInterval: pingInterval,
      connectTimeout: connectTimeout,
    );
  }

  /// Binds an HTTP server on [host] and [port] and accepts upgraded WebSocket connections.
  ///
  /// Throws [UnsupportedError] on Web platforms where server hosting is unavailable.
  static Future<WebSocketNetTransport> bindServer(
    dynamic host,
    int port, {
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    int? serverPeerId,
    Duration pingInterval = const Duration(seconds: 5),
    dynamic securityContext,
  }) {
    return impl.bindWebSocketServer(
      host,
      port,
      authOptions: authOptions,
      serverPeerId: serverPeerId,
      pingInterval: pingInterval,
      securityContext: securityContext,
    );
  }

  /// Local bound port for server, or remote port for client.
  int get port;

  /// Assigned peer ID for this transport endpoint.
  int get peerId;

  /// Target URI if this is a client transport, or `null` if server.
  Uri? get uri;

  /// Active reconnect policy.
  WebSocketReconnectPolicy get reconnectPolicy;

  /// Asynchronously closes the transport, releasing sockets and timers.
  Future<void> closeAsync();

  /// Asynchronously waits until this transport transitions to [NetConnectionState.connected].
  ///
  /// Returns `true` if connected, or `false` on timeout or failure.
  Future<bool> waitUntilConnected({
    Duration timeout = const Duration(seconds: 5),
  });
}
