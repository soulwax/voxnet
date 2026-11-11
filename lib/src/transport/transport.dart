import 'dart:math' as math;
import 'dart:typed_data';

class NetPacket {
  final int senderId;
  final Uint8List data;
  final int timestampMs;

  NetPacket({
    required this.senderId,
    required this.data,
    required this.timestampMs,
  });
}

abstract class NetTransport {
  void send(int recipientId, Uint8List data);
  void broadcast(Uint8List data);
  List<NetPacket> poll([int? currentTimestampMs]);
  void close();
}

class _DelayedPacket {
  final int senderId;
  final int recipientId;
  final Uint8List data;
  final int deliverAtMs;

  _DelayedPacket({
    required this.senderId,
    required this.recipientId,
    required this.data,
    required this.deliverAtMs,
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

  LoopbackEndpoint createEndpoint(int id) {
    final ep = LoopbackEndpoint._(this, id);
    _endpoints[id] = ep;
    return ep;
  }

  void removeEndpoint(int id) {
    _endpoints.remove(id);
    _inFlight.removeWhere((p) => p.senderId == id || p.recipientId == id);
  }

  void _route(int senderId, int recipientId, Uint8List data) {
    if (packetLossRate > 0.0 && _random.nextDouble() < packetLossRate) {
      // Simulated drop
      return;
    }

    var delay = 0;
    if (maxLatencyMs > minLatencyMs) {
      delay = minLatencyMs + _random.nextInt(maxLatencyMs - minLatencyMs);
    } else {
      delay = minLatencyMs;
    }

    _inFlight.add(_DelayedPacket(
      senderId: senderId,
      recipientId: recipientId,
      data: data,
      deliverAtMs: currentTimeMs + delay,
    ));
  }

  List<NetPacket> _pollFor(int endpointId, int? timeMs) {
    final now = timeMs ?? currentTimeMs;
    final ready = <NetPacket>[];

    for (var i = _inFlight.length - 1; i >= 0; i--) {
      final p = _inFlight[i];
      if (p.recipientId == endpointId && p.deliverAtMs <= now) {
        ready.add(NetPacket(
          senderId: p.senderId,
          data: p.data,
          timestampMs: p.deliverAtMs,
        ));
        _inFlight.removeAt(i);
      }
    }

    return ready;
  }
}

class LoopbackEndpoint implements NetTransport {
  final LoopbackNetwork _network;
  final int id;

  LoopbackEndpoint._(this._network, this.id);

  @override
  void send(int recipientId, Uint8List data) {
    _network._route(id, recipientId, data);
  }

  @override
  void broadcast(Uint8List data) {
    for (final recipientId in _network._endpoints.keys) {
      if (recipientId != id) {
        _network._route(id, recipientId, data);
      }
    }
  }

  @override
  List<NetPacket> poll([int? currentTimestampMs]) {
    return _network._pollFor(id, currentTimestampMs);
  }

  @override
  void close() {
    _network.removeEndpoint(id);
  }
}
