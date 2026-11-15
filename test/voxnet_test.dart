import 'package:test/test.dart';
import 'package:voxnet/voxnet.dart';

void main() {
  group('voxnet: Binary Codec & Quantization', () {
    test('ByteWriter and ByteReader serialize and deserialize primitives', () {
      final writer = ByteWriter();
      writer.writeUint8(42);
      writer.writeInt16(-1234);
      writer.writeUint32(999999);
      writer.writeInt64(-9876543210123);
      writer.writeFloat32(3.14159);
      writer.writeFloat64(2.718281828459);
      writer.writeBool(true);
      writer.writeVarInt(1234567);
      writer.writeString('Hello glVolition!');

      final bytes = writer.toBytes();
      final reader = ByteReader(bytes);

      expect(reader.readUint8(), equals(42));
      expect(reader.readInt16(), equals(-1234));
      expect(reader.readUint32(), equals(999999));
      expect(reader.readInt64(), equals(-9876543210123));
      expect(reader.readFloat32(), closeTo(3.14159, 1e-5));
      expect(reader.readFloat64(), closeTo(2.718281828459, 1e-9));
      expect(reader.readBool(), isTrue);
      expect(reader.readVarInt(), equals(1234567));
      expect(reader.readString(), equals('Hello glVolition!'));
      expect(reader.hasRemaining, isFalse);
    });

    test('FloatQuantizer compresses float into integer range and restores', () {
      final quantizer = FloatQuantizer(min: -100.0, max: 100.0, bits: 16);
      const original = 42.5;

      final quantized = quantizer.quantize(original);
      expect(quantized, greaterThan(0));
      expect(quantized, lessThan(65536));

      final restored = quantizer.dequantize(quantized);
      expect(restored, closeTo(original, 0.01));
    });
  });

  group('voxnet: NetMessage & Registry', () {
    setUpAll(() {
      MessageRegistry.register(PingMsg.kTypeId, PingMsg.deserialize);
      MessageRegistry.register(PongMsg.kTypeId, PongMsg.deserialize);
      MessageRegistry.register(InputMsg.kTypeId, InputMsg.deserialize);
      MessageRegistry.register(
        EntitySnapshotMsg.kTypeId,
        EntitySnapshotMsg.deserialize,
      );
      MessageRegistry.register(RpcMsg.kTypeId, RpcMsg.deserialize);
    });

    test('EntitySnapshotMsg serialize and deserialize round-trip', () {
      final msg = EntitySnapshotMsg(
        tick: 105,
        netId: 42,
        x: 10.5,
        y: 20.25,
        z: -5.0,
        qx: 0.0,
        qy: 0.707,
        qz: 0.0,
        qw: 0.707,
        vx: 1.5,
        vy: 0.0,
        vz: -2.0,
      );

      final bytes = MessageRegistry.serialize(msg);
      final restored = MessageRegistry.deserialize(bytes) as EntitySnapshotMsg?;

      expect(restored, isNotNull);
      expect(restored!.tick, equals(105));
      expect(restored.netId, equals(42));
      expect(restored.x, closeTo(10.5, 1e-5));
      expect(restored.y, closeTo(20.25, 1e-5));
      expect(restored.z, closeTo(-5.0, 1e-5));
      expect(restored.qy, closeTo(0.707, 1e-5));
      expect(restored.vx, closeTo(1.5, 1e-5));
    });

    test('RpcMsg serialize and deserialize round-trip', () {
      final payload = ByteWriter();
      payload.writeInt32(500);
      final msg = RpcMsg(
        netId: 1,
        methodName: 'takeDamage',
        payload: payload.toBytes(),
      );

      final bytes = MessageRegistry.serialize(msg);
      final restored = MessageRegistry.deserialize(bytes) as RpcMsg?;

      expect(restored, isNotNull);
      expect(restored!.netId, equals(1));
      expect(restored.methodName, equals('takeDamage'));

      final payloadReader = ByteReader(restored.payload);
      expect(payloadReader.readInt32(), equals(500));
    });
  });

  group('voxnet: Transport & LoopbackNetwork', () {
    test('LoopbackEndpoints route packets immediately without latency', () {
      final net = LoopbackNetwork();
      final epServer = net.createEndpoint(0);
      final epClient = net.createEndpoint(1);

      final writer = ByteWriter();
      writer.writeString('ping');
      epClient.send(0, writer.toBytes());

      final packets = epServer.poll();
      expect(packets, hasLength(1));
      expect(packets.first.senderId, equals(1));

      final reader = ByteReader(packets.first.data);
      expect(reader.readString(), equals('ping'));
    });

    test('LoopbackNetwork respects simulated latency and delivery time', () {
      final net = LoopbackNetwork()
        ..minLatencyMs = 50
        ..maxLatencyMs = 50;

      final epServer = net.createEndpoint(0);
      final epClient = net.createEndpoint(1);

      final writer = ByteWriter();
      writer.writeString('delayed');
      epClient.send(0, writer.toBytes());

      // At t = 0, packet is not ready
      expect(epServer.poll(0), isEmpty);

      // At t = 30, still not ready
      expect(epServer.poll(30), isEmpty);

      // At t = 50, packet arrives!
      final packets = epServer.poll(50);
      expect(packets, hasLength(1));
      expect(ByteReader(packets.first.data).readString(), equals('delayed'));
    });
  });

  group('voxnet: SnapshotBuffer & Interpolation', () {
    test('SnapshotBuffer interpolates between snapshots accurately', () {
      final buffer = SnapshotBuffer<double>();

      // Snapshot at t=0ms, val=0.0
      buffer.add(const Snapshot(tick: 0, timestampMs: 0, state: 0.0));
      // Snapshot at t=100ms, val=100.0
      buffer.add(const Snapshot(tick: 1, timestampMs: 100, state: 100.0));

      double lerp(double from, double to, double alpha) =>
          from + (to - from) * alpha;

      // Sample at t=25ms -> should be 25.0
      final s25 = buffer.sample(25, lerp);
      expect(s25, closeTo(25.0, 1e-6));

      // Sample at t=75ms -> should be 75.0
      final s75 = buffer.sample(75, lerp);
      expect(s75, closeTo(75.0, 1e-6));

      // Sample before oldest -> clamps to oldest
      expect(buffer.sample(-10, lerp), closeTo(0.0, 1e-6));

      // Sample after newest -> clamps to newest
      expect(buffer.sample(150, lerp), closeTo(100.0, 1e-6));
    });
  });

  group('voxnet: Client-Side Prediction & Server Reconciliation (Rollback)', () {
    test('PredictionHistory rolls back and re-simulates on divergence', () {
      final history = PredictionHistory<double, double>();

      // Client simulates 3 ticks with input = +10 per tick
      // Tick 1: state = 10
      history.record(1, 10.0, 10.0);
      // Tick 2: state = 20
      history.record(2, 10.0, 20.0);
      // Tick 3: state = 30
      history.record(3, 10.0, 30.0);

      // Server authoritative state arrives for Tick 1:
      // Due to external hit or obstacle, server says state at Tick 1 was actually 5.0 (not 10.0)!
      final result = history.reconcile(
        serverTick: 1,
        authoritativeState: 5.0,
        hasDiverged: (predicted, authoritative) =>
            (predicted - authoritative).abs() > 0.01,
        simulateStep: (current, input, tick) => current + input,
      );

      expect(result, isNotNull);
      expect(result!.rolledBack, isTrue);
      expect(result.rollbackTick, equals(1));
      expect(result.replayedTicksCount, equals(2)); // Replayed ticks 2 and 3
      // Re-simulation: Tick 1 = 5.0 -> Tick 2 = 5 + 10 = 15.0 -> Tick 3 = 15 + 10 = 25.0!
      expect(result.correctedState, equals(25.0));
      expect(history.latest!.state, equals(25.0));
    });

    test(
      'PredictionHistory does not rollback when predicted state matches server',
      () {
        final history = PredictionHistory<double, double>();

        history.record(1, 10.0, 10.0);
        history.record(2, 10.0, 20.0);

        final result = history.reconcile(
          serverTick: 1,
          authoritativeState: 10.0, // Matches perfectly!
          hasDiverged: (predicted, authoritative) =>
              (predicted - authoritative).abs() > 0.01,
          simulateStep: (current, input, tick) => current + input,
        );

        expect(result, isNotNull);
        expect(result!.rolledBack, isFalse);
        expect(result.replayedTicksCount, equals(0));
        expect(result.correctedState, equals(20.0));
      },
    );
  });

  group('voxnet: DeltaCompressor & RPC', () {
    test('DeltaCompressor bitmask tracks dirty changes', () {
      final compressor = DeltaCompressor();
      expect(compressor.hasChanges, isFalse);

      compressor.markDirty(0); // Position dirty
      compressor.markDirty(3); // Health dirty

      expect(compressor.isDirty(0), isTrue);
      expect(compressor.isDirty(1), isFalse);
      expect(compressor.isDirty(3), isTrue);

      final writer = ByteWriter();
      compressor.writeMask(writer);

      final reader = ByteReader(writer.toBytes());
      final readMask = DeltaCompressor.readMask(reader);
      expect(DeltaCompressor.isFieldInMask(readMask, 0), isTrue);
      expect(DeltaCompressor.isFieldInMask(readMask, 1), isFalse);
      expect(DeltaCompressor.isFieldInMask(readMask, 3), isTrue);
    });

    test('RpcManager registers and dispatches remote procedure calls', () {
      final rpc = RpcManager();
      var called = false;
      var receivedValue = 0;

      rpc.register('heal', (senderId, payload) {
        called = true;
        receivedValue = ByteReader(payload).readInt32();
      });

      final payload = ByteWriter()..writeInt32(75);
      final dispatched = rpc.dispatch(2, 'heal', payload.toBytes());

      expect(dispatched, isTrue);
      expect(called, isTrue);
      expect(receivedValue, equals(75));

      expect(rpc.dispatch(2, 'unknownMethod', payload.toBytes()), isFalse);
    });
  });
}
