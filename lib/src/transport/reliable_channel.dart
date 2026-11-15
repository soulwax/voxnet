import 'dart:collection';
import 'dart:math' as math;
import 'dart:typed_data';

import 'transport.dart';

/// Queued datagram item held by [UdpFaultQueue] until [deliverAtMs].
class UdpDelayedDatagram<T> {
  final T target;
  final Uint8List payload;
  final int deliverAtMs;

  const UdpDelayedDatagram({
    required this.target,
    required this.payload,
    required this.deliverAtMs,
  });
}

/// Deterministic synthetic network fault injector (packet drop, duplication,
/// latency jitter, and packet reordering) for UDP transport tests.
class UdpFaultInjector {
  final math.Random _random;
  final double dropProbability;
  final double duplicateProbability;
  final double reorderProbability;
  final int minLatencyMs;
  final int maxJitterMs;
  final int maxReorderDelayMs;
  int droppedCount = 0;
  int duplicatedCount = 0;
  int reorderedCount = 0;

  UdpFaultInjector({
    this.dropProbability = 0.0,
    this.duplicateProbability = 0.0,
    this.reorderProbability = 0.0,
    this.minLatencyMs = 0,
    this.maxJitterMs = 0,
    this.maxReorderDelayMs = 25,
    int seed = 42,
  }) : _random = math.Random(seed);

  bool get isPassThrough =>
      dropProbability <= 0.0 &&
      duplicateProbability <= 0.0 &&
      reorderProbability <= 0.0 &&
      minLatencyMs <= 0 &&
      maxJitterMs <= 0;

  /// Evaluates faults for an outbound [datagram] at [nowMs] and returns 0, 1,
  /// or 2 delivery delays (in milliseconds from [nowMs]).
  ///
  /// Returning an empty list indicates the packet should be dropped.
  List<int> sampleDeliveryDelays({NetTransportMetrics? metrics}) {
    if (dropProbability > 0.0 && _random.nextDouble() < dropProbability) {
      droppedCount++;
      metrics?.recordDrop();
      return const [];
    }

    var baseDelay = minLatencyMs;
    if (maxJitterMs > 0) {
      baseDelay += _random.nextInt(maxJitterMs + 1);
    }
    if (reorderProbability > 0.0 &&
        maxReorderDelayMs > 0 &&
        _random.nextDouble() < reorderProbability) {
      baseDelay += 1 + _random.nextInt(maxReorderDelayMs);
      reorderedCount++;
    }

    if (duplicateProbability > 0.0 &&
        _random.nextDouble() < duplicateProbability) {
      duplicatedCount++;
      return [baseDelay, baseDelay + 2];
    }

    return [baseDelay];
  }
}

/// Bounded queue applying [UdpFaultInjector] delays before dispatching packets.
class UdpFaultQueue<T> {
  final UdpFaultInjector injector;
  final List<UdpDelayedDatagram<T>> _pending = [];

  UdpFaultQueue(this.injector);

  int get pendingCount => _pending.length;

  void enqueue(
    T target,
    Uint8List payload,
    int nowMs, {
    NetTransportMetrics? metrics,
  }) {
    final delays = injector.sampleDeliveryDelays(metrics: metrics);
    for (final d in delays) {
      _pending.add(
        UdpDelayedDatagram<T>(
          target: target,
          payload: Uint8List.fromList(payload),
          deliverAtMs: nowMs + d,
        ),
      );
    }
  }

  List<UdpDelayedDatagram<T>> drainReady(int nowMs) {
    if (_pending.isEmpty) return const [];
    final ready = <UdpDelayedDatagram<T>>[];
    for (var i = _pending.length - 1; i >= 0; i--) {
      if (_pending[i].deliverAtMs <= nowMs) {
        ready.add(_pending.removeAt(i));
      }
    }
    // Reverse so earlier queued ready items maintain order unless delayed.
    return ready.reversed.toList();
  }
}

/// Binary UDP datagram header and payload frame used by [ReliableChannelEndpoint].
///
/// Layout (30 bytes header + payload):
/// - [0..1]   uint16 magic `0x564E` (`'VN'`)
/// - [2]      uint8 frame type ([typeData], [typeAckOnly], [typeHandshakeSyn], [typeHandshakeAck], [typeDisconnect])
/// - [3]      uint8 [NetDeliveryChannel.index]
/// - [4..7]   int32 senderId
/// - [8..11]  uint32 packet sequence
/// - [12..15] uint32 cumulative remote sequence ack
/// - [16..19] uint32 32-bit prior ack bitmask (`ack - 1 .. ack - 32`)
/// - [20..23] uint32 per-channel logical message sequence
/// - [24..25] uint16 fragment index
/// - [26..27] uint16 fragment count (`>= 1`)
/// - [28..29] uint16 payload length
class ReliableChannelFrame {
  static const int magic = 0x564E;
  static const int headerSize = 30;

  static const int typeData = 0;
  static const int typeAckOnly = 1;
  static const int typeHandshakeSyn = 2;
  static const int typeHandshakeAck = 3;
  static const int typeDisconnect = 4;

  final int type;
  final NetDeliveryChannel channel;
  final int senderId;
  final int sequence;
  final int ack;
  final int ackBits;
  final int messageSeq;
  final int fragmentIndex;
  final int fragmentCount;
  final Uint8List payload;

  const ReliableChannelFrame({
    required this.type,
    required this.channel,
    required this.senderId,
    required this.sequence,
    required this.ack,
    required this.ackBits,
    required this.messageSeq,
    required this.fragmentIndex,
    required this.fragmentCount,
    required this.payload,
  });

  Uint8List encode() {
    final out = Uint8List(headerSize + payload.length);
    final bd = ByteData.sublistView(out);
    bd.setUint16(0, magic, Endian.little);
    bd.setUint8(2, type);
    bd.setUint8(3, channel.index);
    bd.setInt32(4, senderId, Endian.little);
    bd.setUint32(8, sequence, Endian.little);
    bd.setUint32(12, ack, Endian.little);
    bd.setUint32(16, ackBits, Endian.little);
    bd.setUint32(20, messageSeq, Endian.little);
    bd.setUint16(24, fragmentIndex, Endian.little);
    bd.setUint16(26, fragmentCount, Endian.little);
    bd.setUint16(28, payload.length, Endian.little);
    if (payload.isNotEmpty) {
      out.setRange(headerSize, headerSize + payload.length, payload);
    }
    return out;
  }

  static ReliableChannelFrame? decode(Uint8List bytes) {
    if (bytes.length < headerSize) return null;
    final bd = ByteData.sublistView(bytes);
    if (bd.getUint16(0, Endian.little) != magic) return null;

    final type = bd.getUint8(2);
    final channelIndex = bd.getUint8(3);
    if (channelIndex >= NetDeliveryChannel.values.length) return null;

    final senderId = bd.getInt32(4, Endian.little);
    final sequence = bd.getUint32(8, Endian.little);
    final ack = bd.getUint32(12, Endian.little);
    final ackBits = bd.getUint32(16, Endian.little);
    final messageSeq = bd.getUint32(20, Endian.little);
    final fragmentIndex = bd.getUint16(24, Endian.little);
    final fragmentCount = bd.getUint16(26, Endian.little);
    final payloadLength = bd.getUint16(28, Endian.little);

    if (headerSize + payloadLength > bytes.length) return null;
    final payload = Uint8List.fromList(
      Uint8List.sublistView(bytes, headerSize, headerSize + payloadLength),
    );

    return ReliableChannelFrame(
      type: type,
      channel: NetDeliveryChannel.values[channelIndex],
      senderId: senderId,
      sequence: sequence,
      ack: ack,
      ackBits: ackBits,
      messageSeq: messageSeq,
      fragmentIndex: fragmentIndex,
      fragmentCount: fragmentCount,
      payload: payload,
    );
  }
}

class _OutgoingChunk {
  final NetDeliveryChannel channel;
  final int packetSeq;
  final int messageSeq;
  final int fragmentIndex;
  final int fragmentCount;
  final Uint8List payload;
  final int enqueuedAtMs;

  _OutgoingChunk({
    required this.channel,
    required this.packetSeq,
    required this.messageSeq,
    required this.fragmentIndex,
    required this.fragmentCount,
    required this.payload,
    required this.enqueuedAtMs,
  });
}

class _InFlightReliable {
  final _OutgoingChunk chunk;
  int lastSentAtMs;
  int retries = 0;

  _InFlightReliable({required this.chunk, required this.lastSentAtMs});
}

class _FragmentAssembly {
  final NetDeliveryChannel channel;
  final int messageSeq;
  final int fragmentCount;
  final int createdAtMs;
  final List<Uint8List?> parts;
  int receivedCount = 0;

  _FragmentAssembly({
    required this.channel,
    required this.messageSeq,
    required this.fragmentCount,
    required this.createdAtMs,
  }) : parts = List<Uint8List?>.filled(fragmentCount, null);

  bool addPart(int index, Uint8List bytes) {
    if (index < 0 || index >= fragmentCount) return false;
    if (parts[index] == null) {
      parts[index] = bytes;
      receivedCount++;
    }
    return receivedCount == fragmentCount;
  }

  Uint8List assemble() {
    var totalLength = 0;
    for (final p in parts) {
      if (p != null) totalLength += p.length;
    }
    final merged = Uint8List(totalLength);
    var offset = 0;
    for (final p in parts) {
      if (p == null) continue;
      merged.setRange(offset, offset + p.length, p);
      offset += p.length;
    }
    return merged;
  }
}

/// Per-peer UDP channel multiplexer handling 4 [NetDeliveryChannel] semantics,
/// 32-bit ACK bitfields, AIMD congestion windowing, RTO retransmission, and
/// MTU payload fragmentation/reassembly.
class ReliableChannelEndpoint {
  final int localPeerId;
  final int remotePeerId;
  final int maxMtuBytes;
  final int minCongestionWindow;
  final int maxCongestionWindow;
  final NetTransportMetrics? metrics;

  int congestionWindow;
  int _nextPacketSeq = 1;
  final List<int> _nextMessageSeqByChannel = List<int>.filled(
    NetDeliveryChannel.values.length,
    1,
  );

  // Inbound ACK generation state
  bool _hasReceivedPacket = false;
  int _remoteAck = 0;
  int _remoteAckBits = 0;
  bool _needsAck = false;

  // Outbound queues & in-flight reliability tracking
  final Queue<_OutgoingChunk> _pendingReliable = Queue<_OutgoingChunk>();
  final List<_OutgoingChunk> _pendingUnreliable = [];
  final Map<int, _InFlightReliable> _inFlightReliable = {};

  // Inbound reassembly & ordering state
  final Map<int, _FragmentAssembly> _reassembly = {};
  int _orderedNextMessageSeq = 1;
  final Map<int, _ReceivedOrderedMessage> _orderedBuffer = {};
  final Queue<int> _unorderedSeenOrder = Queue<int>();
  final Set<int> _unorderedSeenSet = {};
  bool _hasUnreliableSequenced = false;
  int _latestUnreliableSequencedMsgSeq = 0;

  ReliableChannelEndpoint({
    required this.localPeerId,
    required this.remotePeerId,
    this.maxMtuBytes = 1200,
    this.minCongestionWindow = 2,
    int initialCongestionWindow = 16,
    this.maxCongestionWindow = 64,
    this.metrics,
  }) : congestionWindow = initialCongestionWindow.clamp(
         minCongestionWindow,
         maxCongestionWindow,
       );

  int get maxPayloadPerDatagram =>
      math.max(16, maxMtuBytes - ReliableChannelFrame.headerSize);

  int get inFlightCount => _inFlightReliable.length;
  int get pendingReliableCount => _pendingReliable.length;

  /// Computes adaptive Retransmission Timeout (RTO) from smoothed RTT.
  double get rtoMs {
    final srtt = metrics?.smoothedRttMs ?? 0.0;
    if (srtt <= 0.0) return 60.0;
    return (srtt * 2.0 + 30.0).clamp(25.0, 1200.0);
  }

  /// Unsigned 32-bit wrap-aware sequence comparison (`s1 > s2`).
  static bool isSequenceNewer(int s1, int s2) {
    final a = s1 & 0xFFFFFFFF;
    final b = s2 & 0xFFFFFFFF;
    return ((a > b) && (a - b <= 0x7FFFFFFF)) ||
        ((a < b) && (b - a > 0x7FFFFFFF));
  }

  /// Unsigned 32-bit modular distance (`a - b`).
  static int sequenceDiff(int a, int b) => (a - b) & 0xFFFFFFFF;

  /// Enqueues [payload] on [channel], automatically fragmenting above
  /// [maxPayloadPerDatagram] bytes.
  void queueSend(
    Uint8List payload, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
    int nowMs = 0,
  }) {
    final mtuPayload = maxPayloadPerDatagram;
    final fragmentCount = payload.isEmpty
        ? 1
        : (payload.length + mtuPayload - 1) ~/ mtuPayload;
    final msgSeq = _nextMessageSeqByChannel[channel.index];
    _nextMessageSeqByChannel[channel.index] = (msgSeq + 1) & 0xFFFFFFFF;

    for (var i = 0; i < fragmentCount; i++) {
      final start = i * mtuPayload;
      final end = math.min(payload.length, start + mtuPayload);
      final slice = payload.isEmpty
          ? Uint8List(0)
          : Uint8List.fromList(Uint8List.sublistView(payload, start, end));
      final packetSeq = _nextPacketSeq;
      _nextPacketSeq = (_nextPacketSeq + 1) & 0xFFFFFFFF;
      if (_nextPacketSeq == 0) _nextPacketSeq = 1;

      final chunk = _OutgoingChunk(
        channel: channel,
        packetSeq: packetSeq,
        messageSeq: msgSeq,
        fragmentIndex: i,
        fragmentCount: fragmentCount,
        payload: slice,
        enqueuedAtMs: nowMs,
      );

      if (_isReliable(channel)) {
        _pendingReliable.add(chunk);
      } else {
        _pendingUnreliable.add(chunk);
      }
    }
  }

  /// Collects newly scheduled, retransmitted, and ACK-only UDP datagrams ready
  /// to transmit at [nowMs].
  List<Uint8List> collectOutgoingDatagrams(int nowMs) {
    final datagrams = <Uint8List>[];
    final currentRto = rtoMs;

    // 1. Retransmit expired in-flight reliable chunks and back off AIMD window.
    var backedOff = false;
    for (final entry in _inFlightReliable.values) {
      if (nowMs - entry.lastSentAtMs >= currentRto) {
        entry.lastSentAtMs = nowMs;
        entry.retries++;
        metrics?.recordRetransmit();
        if (!backedOff) {
          congestionWindow = math.max(
            minCongestionWindow,
            congestionWindow ~/ 2,
          );
          backedOff = true;
        }
        datagrams.add(_encodeChunk(entry.chunk));
      }
    }

    // 2. Transmit pending unreliable chunks immediately.
    if (_pendingUnreliable.isNotEmpty) {
      for (final chunk in _pendingUnreliable) {
        datagrams.add(_encodeChunk(chunk));
      }
      _pendingUnreliable.clear();
    }

    // 3. Transmit queued reliable chunks up to congestionWindow.
    while (_pendingReliable.isNotEmpty &&
        _inFlightReliable.length < congestionWindow) {
      final chunk = _pendingReliable.removeFirst();
      _inFlightReliable[chunk.packetSeq] = _InFlightReliable(
        chunk: chunk,
        lastSentAtMs: nowMs,
      );
      datagrams.add(_encodeChunk(chunk));
    }

    // 4. Emit lightweight ackOnly packet if we received packets and sent nothing.
    if (_needsAck && _hasReceivedPacket && datagrams.isEmpty) {
      datagrams.add(
        ReliableChannelFrame(
          type: ReliableChannelFrame.typeAckOnly,
          channel: NetDeliveryChannel.unreliable,
          senderId: localPeerId,
          sequence: 0,
          ack: _remoteAck,
          ackBits: _remoteAckBits,
          messageSeq: 0,
          fragmentIndex: 0,
          fragmentCount: 0,
          payload: Uint8List(0),
        ).encode(),
      );
    }

    if (datagrams.isNotEmpty) {
      _needsAck = false;
    }

    // Evict stale incomplete fragments after 5 seconds.
    _reassembly.removeWhere((_, asm) => nowMs - asm.createdAtMs > 5000);

    return datagrams;
  }

  /// Decodes and processes a raw UDP datagram received at [nowMs], returning
  /// zero or more ordered/reassembled [NetPacket] deliveries.
  List<NetPacket> receiveDatagram(Uint8List datagram, int nowMs) {
    final frame = ReliableChannelFrame.decode(datagram);
    if (frame == null) return const [];
    return receiveFrame(frame, nowMs);
  }

  /// Processes an already-decoded [ReliableChannelFrame] received at [nowMs].
  List<NetPacket> receiveFrame(ReliableChannelFrame frame, int nowMs) {
    _processIncomingAck(frame.ack, frame.ackBits, nowMs);

    if (frame.type != ReliableChannelFrame.typeData) {
      return const [];
    }

    _recordReceivedSequence(frame.sequence);

    Uint8List? fullPayload;
    if (frame.fragmentCount <= 1) {
      fullPayload = frame.payload;
    } else {
      final key =
          ((frame.channel.index & 0xF) << 32) | (frame.messageSeq & 0xFFFFFFFF);
      final asm = _reassembly.putIfAbsent(
        key,
        () => _FragmentAssembly(
          channel: frame.channel,
          messageSeq: frame.messageSeq,
          fragmentCount: frame.fragmentCount,
          createdAtMs: nowMs,
        ),
      );
      if (asm.addPart(frame.fragmentIndex, frame.payload)) {
        _reassembly.remove(key);
        fullPayload = asm.assemble();
      }
    }

    if (fullPayload == null) return const [];

    switch (frame.channel) {
      case NetDeliveryChannel.reliableOrdered:
        if (isSequenceNewer(_orderedNextMessageSeq, frame.messageSeq)) {
          // Duplicate older than next expected ordered message.
          return const [];
        }
        _orderedBuffer.putIfAbsent(
          frame.messageSeq,
          () => _ReceivedOrderedMessage(
            senderId: frame.senderId,
            data: fullPayload!,
            timestampMs: nowMs,
          ),
        );
        final delivered = <NetPacket>[];
        while (_orderedBuffer.containsKey(_orderedNextMessageSeq)) {
          final msg = _orderedBuffer.remove(_orderedNextMessageSeq)!;
          delivered.add(
            NetPacket(
              senderId: msg.senderId,
              data: msg.data,
              timestampMs: msg.timestampMs,
              channel: NetDeliveryChannel.reliableOrdered,
            ),
          );
          _orderedNextMessageSeq = (_orderedNextMessageSeq + 1) & 0xFFFFFFFF;
        }
        return delivered;

      case NetDeliveryChannel.reliableUnordered:
        if (_unorderedSeenSet.contains(frame.messageSeq)) {
          return const [];
        }
        _unorderedSeenSet.add(frame.messageSeq);
        _unorderedSeenOrder.addLast(frame.messageSeq);
        while (_unorderedSeenOrder.length > 512) {
          _unorderedSeenSet.remove(_unorderedSeenOrder.removeFirst());
        }
        return [
          NetPacket(
            senderId: frame.senderId,
            data: fullPayload,
            timestampMs: nowMs,
            channel: NetDeliveryChannel.reliableUnordered,
          ),
        ];

      case NetDeliveryChannel.unreliableSequenced:
        if (_hasUnreliableSequenced &&
            !isSequenceNewer(
              frame.messageSeq,
              _latestUnreliableSequencedMsgSeq,
            )) {
          return const [];
        }
        _hasUnreliableSequenced = true;
        _latestUnreliableSequencedMsgSeq = frame.messageSeq;
        return [
          NetPacket(
            senderId: frame.senderId,
            data: fullPayload,
            timestampMs: nowMs,
            channel: NetDeliveryChannel.unreliableSequenced,
          ),
        ];

      case NetDeliveryChannel.unreliable:
        return [
          NetPacket(
            senderId: frame.senderId,
            data: fullPayload,
            timestampMs: nowMs,
            channel: NetDeliveryChannel.unreliable,
          ),
        ];
    }
  }

  Uint8List _encodeChunk(_OutgoingChunk chunk) => ReliableChannelFrame(
    type: ReliableChannelFrame.typeData,
    channel: chunk.channel,
    senderId: localPeerId,
    sequence: chunk.packetSeq,
    ack: _remoteAck,
    ackBits: _remoteAckBits,
    messageSeq: chunk.messageSeq,
    fragmentIndex: chunk.fragmentIndex,
    fragmentCount: chunk.fragmentCount,
    payload: chunk.payload,
  ).encode();

  void _processIncomingAck(int ack, int ackBits, int nowMs) {
    if (ack == 0 && ackBits == 0) return;
    var newlyAcked = 0;

    final ackedKeys = <int>[];
    for (final seq in _inFlightReliable.keys) {
      if (seq == ack) {
        ackedKeys.add(seq);
      } else if (isSequenceNewer(ack, seq)) {
        final diff = sequenceDiff(ack, seq);
        if (diff >= 1 && diff <= 32 && ((ackBits & (1 << (diff - 1))) != 0)) {
          ackedKeys.add(seq);
        }
      }
    }

    for (final seq in ackedKeys) {
      final entry = _inFlightReliable.remove(seq);
      if (entry != null) {
        newlyAcked++;
        if (entry.retries == 0) {
          metrics?.recordRtt((nowMs - entry.lastSentAtMs).toDouble());
        }
      }
    }

    if (newlyAcked > 0) {
      congestionWindow = math.min(
        maxCongestionWindow,
        congestionWindow + newlyAcked,
      );
    }
  }

  void _recordReceivedSequence(int sequence) {
    if (sequence == 0) return;
    _needsAck = true;
    if (!_hasReceivedPacket) {
      _hasReceivedPacket = true;
      _remoteAck = sequence;
      _remoteAckBits = 0;
      return;
    }

    if (sequence == _remoteAck) return;

    if (isSequenceNewer(sequence, _remoteAck)) {
      final diff = sequenceDiff(sequence, _remoteAck);
      if (diff <= 32) {
        _remoteAckBits =
            ((_remoteAckBits << diff) | (1 << (diff - 1))) & 0xFFFFFFFF;
      } else {
        _remoteAckBits = 0;
      }
      _remoteAck = sequence;
    } else {
      final diff = sequenceDiff(_remoteAck, sequence);
      if (diff >= 1 && diff <= 32) {
        _remoteAckBits = (_remoteAckBits | (1 << (diff - 1))) & 0xFFFFFFFF;
      }
    }
  }

  static bool _isReliable(NetDeliveryChannel channel) =>
      channel == NetDeliveryChannel.reliableOrdered ||
      channel == NetDeliveryChannel.reliableUnordered;
}

class _ReceivedOrderedMessage {
  final int senderId;
  final Uint8List data;
  final int timestampMs;

  const _ReceivedOrderedMessage({
    required this.senderId,
    required this.data,
    required this.timestampMs,
  });
}
