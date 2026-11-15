import 'dart:math' as math;
import 'dart:typed_data';

/// Lifecycle states for a [NetTransport] peer or server session.
enum NetConnectionState {
  disconnected,
  connecting,
  connected,
  reconnecting,
  closing,
  failed,
}

/// Delivery semantics hint supported by multi-channel transports.
enum NetDeliveryChannel {
  reliableOrdered,
  reliableUnordered,
  unreliableSequenced,
  unreliable,
}

/// TLS, bearer token, and custom header authentication options for transports.
class NetTransportAuthOptions {
  final String? bearerToken;
  final Map<String, String> headers;
  final bool useTls;
  final String? tlsCertificatePath;
  final bool allowInsecureLocalhostTls;

  const NetTransportAuthOptions({
    this.bearerToken,
    this.headers = const {},
    this.useTls = false,
    this.tlsCertificatePath,
    this.allowInsecureLocalhostTls = false,
  });

  /// Returns merged HTTP/handshake headers including `Authorization` if a
  /// [bearerToken] is configured.
  Map<String, String> resolvedHeaders() {
    final result = Map<String, String>.from(headers);
    final token = bearerToken;
    if (token != null && token.isNotEmpty) {
      result.putIfAbsent('Authorization', () => 'Bearer $token');
    }
    return Map<String, String>.unmodifiable(result);
  }
}

/// Immutable telemetry snapshot captured from a [NetTransportMetrics] instance.
class NetTransportMetricsSnapshot {
  final int packetsSent;
  final int packetsReceived;
  final int bytesSent;
  final int bytesReceived;
  final int packetsDropped;
  final int packetsRetransmitted;
  final int connectAttempts;
  final int reconnectCount;
  final int authFailures;
  final double lastRttMs;
  final double smoothedRttMs;
  final double jitterMs;
  final double packetLossRate;

  const NetTransportMetricsSnapshot({
    required this.packetsSent,
    required this.packetsReceived,
    required this.bytesSent,
    required this.bytesReceived,
    required this.packetsDropped,
    required this.packetsRetransmitted,
    required this.connectAttempts,
    required this.reconnectCount,
    required this.authFailures,
    required this.lastRttMs,
    required this.smoothedRttMs,
    required this.jitterMs,
    required this.packetLossRate,
  });

  Map<String, Object?> toJson() => {
    'packetsSent': packetsSent,
    'packetsReceived': packetsReceived,
    'bytesSent': bytesSent,
    'bytesReceived': bytesReceived,
    'packetsDropped': packetsDropped,
    'packetsRetransmitted': packetsRetransmitted,
    'connectAttempts': connectAttempts,
    'reconnectCount': reconnectCount,
    'authFailures': authFailures,
    'lastRttMs': lastRttMs,
    'smoothedRttMs': smoothedRttMs,
    'jitterMs': jitterMs,
    'packetLossRate': packetLossRate,
  };
}

/// Mutable RTT, throughput, and packet-loss counters owned by a [NetTransport].
class NetTransportMetrics {
  int packetsSent = 0;
  int packetsReceived = 0;
  int bytesSent = 0;
  int bytesReceived = 0;
  int packetsDropped = 0;
  int packetsRetransmitted = 0;
  int connectAttempts = 0;
  int reconnectCount = 0;
  int authFailures = 0;
  double lastRttMs = 0.0;
  double smoothedRttMs = 0.0;
  double jitterMs = 0.0;
  DateTime? lastActivityUtc;
  bool _hasRttSample = false;

  double get packetLossRate {
    final total = packetsSent + packetsDropped;
    if (total <= 0) return 0.0;
    return packetsDropped / total;
  }

  void recordSend(int byteLength, [int packetCount = 1]) {
    packetsSent += packetCount;
    bytesSent += byteLength;
    lastActivityUtc = DateTime.now().toUtc();
  }

  void recordReceive(int byteLength, [int packetCount = 1]) {
    packetsReceived += packetCount;
    bytesReceived += byteLength;
    lastActivityUtc = DateTime.now().toUtc();
  }

  void recordDrop([int count = 1]) {
    packetsDropped += count;
  }

  void recordRetransmit([int count = 1]) {
    packetsRetransmitted += count;
  }

  void recordRtt(double sampleMs) {
    final safe = sampleMs < 0 ? 0.0 : sampleMs;
    if (!_hasRttSample) {
      _hasRttSample = true;
      lastRttMs = safe;
      smoothedRttMs = safe;
      jitterMs = 0.0;
      return;
    }
    final diff = (safe - lastRttMs).abs();
    lastRttMs = safe;
    smoothedRttMs = smoothedRttMs * 0.875 + safe * 0.125;
    jitterMs = jitterMs * 0.75 + diff * 0.25;
  }

  void reset() {
    packetsSent = 0;
    packetsReceived = 0;
    bytesSent = 0;
    bytesReceived = 0;
    packetsDropped = 0;
    packetsRetransmitted = 0;
    connectAttempts = 0;
    reconnectCount = 0;
    authFailures = 0;
    lastRttMs = 0.0;
    smoothedRttMs = 0.0;
    jitterMs = 0.0;
    lastActivityUtc = null;
    _hasRttSample = false;
  }

  NetTransportMetricsSnapshot snapshot() => NetTransportMetricsSnapshot(
    packetsSent: packetsSent,
    packetsReceived: packetsReceived,
    bytesSent: bytesSent,
    bytesReceived: bytesReceived,
    packetsDropped: packetsDropped,
    packetsRetransmitted: packetsRetransmitted,
    connectAttempts: connectAttempts,
    reconnectCount: reconnectCount,
    authFailures: authFailures,
    lastRttMs: lastRttMs,
    smoothedRttMs: smoothedRttMs,
    jitterMs: jitterMs,
    packetLossRate: packetLossRate,
  );
}

class NetPacket {
  final int senderId;
  final Uint8List data;
  final int timestampMs;
  final NetDeliveryChannel channel;

  NetPacket({
    required this.senderId,
    required this.data,
    required this.timestampMs,
    this.channel = NetDeliveryChannel.reliableOrdered,
  });
}

abstract class NetTransport {
  NetConnectionState get connectionState;
  NetTransportAuthOptions get authOptions;
  NetTransportMetrics get metrics;
  List<int> get connectedPeerIds;

  void send(int recipientId, Uint8List data);
  void broadcast(Uint8List data);

  void sendWithChannel(
    int recipientId,
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  });

  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  });

  List<NetPacket> poll([int? currentTimestampMs]);
  void close();
}

class _DelayedPacket {
  final int senderId;
  final int recipientId;
  final Uint8List data;
  final int deliverAtMs;
  final NetDeliveryChannel channel;

  _DelayedPacket({
    required this.senderId,
    required this.recipientId,
    required this.data,
    required this.deliverAtMs,
    required this.channel,
  });
}

/// In-memory loopback transport simulator with configurable latency, jitter, and packet loss.
class LoopbackNetwork {
  final math.Random _random = math.Random(1337);
  final List<_DelayedPacket> _inFlight = [];
  final Map<int, LoopbackEndpoint> _endpoints = {};

  int currentTimeMs = 0;
  double packetLossRate = 0.0;
  int minLatencyMs = 0;
  int maxLatencyMs = 0;

  LoopbackEndpoint createEndpoint(
    int id, {
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
  }) {
    final ep = LoopbackEndpoint._(this, id, authOptions);
    _endpoints[id] = ep;
    return ep;
  }

  void removeEndpoint(int id) {
    _endpoints.remove(id);
    _inFlight.removeWhere((p) => p.senderId == id || p.recipientId == id);
  }

  void _route(
    int senderId,
    int recipientId,
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    final sender = _endpoints[senderId];
    sender?.metrics.recordSend(data.length);

    if (packetLossRate > 0.0 && _random.nextDouble() < packetLossRate) {
      sender?.metrics.recordDrop();
      return;
    }

    var delay = 0;
    if (maxLatencyMs > minLatencyMs) {
      delay = minLatencyMs + _random.nextInt(maxLatencyMs - minLatencyMs);
    } else {
      delay = minLatencyMs;
    }

    _inFlight.add(
      _DelayedPacket(
        senderId: senderId,
        recipientId: recipientId,
        data: data,
        deliverAtMs: currentTimeMs + delay,
        channel: channel,
      ),
    );
  }

  List<NetPacket> _pollFor(int endpointId, int? timeMs) {
    final now = timeMs ?? currentTimeMs;
    final receiver = _endpoints[endpointId];
    final ready = <NetPacket>[];

    for (var i = _inFlight.length - 1; i >= 0; i--) {
      final p = _inFlight[i];
      if (p.recipientId == endpointId && p.deliverAtMs <= now) {
        receiver?.metrics.recordReceive(p.data.length);
        ready.add(
          NetPacket(
            senderId: p.senderId,
            data: p.data,
            timestampMs: p.deliverAtMs,
            channel: p.channel,
          ),
        );
        _inFlight.removeAt(i);
      }
    }

    return ready;
  }
}

class LoopbackEndpoint implements NetTransport {
  final LoopbackNetwork _network;
  final int id;
  @override
  final NetTransportAuthOptions authOptions;
  @override
  final NetTransportMetrics metrics = NetTransportMetrics();
  NetConnectionState _state = NetConnectionState.connected;

  LoopbackEndpoint._(
    this._network,
    this.id, [
    this.authOptions = const NetTransportAuthOptions(),
  ]);

  @override
  NetConnectionState get connectionState => _state;

  @override
  List<int> get connectedPeerIds => [
    for (final key in _network._endpoints.keys)
      if (key != id) key,
  ];

  @override
  void send(int recipientId, Uint8List data) {
    if (_state != NetConnectionState.connected) return;
    _network._route(id, recipientId, data);
  }

  @override
  void broadcast(Uint8List data) {
    if (_state != NetConnectionState.connected) return;
    for (final recipientId in _network._endpoints.keys) {
      if (recipientId != id) {
        _network._route(id, recipientId, data);
      }
    }
  }

  @override
  void sendWithChannel(
    int recipientId,
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    if (_state != NetConnectionState.connected) return;
    _network._route(id, recipientId, data, channel: channel);
  }

  @override
  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {
    if (_state != NetConnectionState.connected) return;
    for (final recipientId in _network._endpoints.keys) {
      if (recipientId != id) {
        _network._route(id, recipientId, data, channel: channel);
      }
    }
  }

  @override
  List<NetPacket> poll([int? currentTimestampMs]) {
    if (_state == NetConnectionState.disconnected) return const [];
    return _network._pollFor(id, currentTimestampMs);
  }

  @override
  void close() {
    _state = NetConnectionState.disconnected;
    _network.removeEndpoint(id);
  }
}
