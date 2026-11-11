import 'dart:typed_data';
import '../codec/byte_buffer.dart';

abstract class NetMessage {
  int get typeId;
  void serialize(ByteWriter writer);
}

typedef MessageFactory = NetMessage Function(ByteReader reader);

class MessageRegistry {
  static final Map<int, MessageFactory> _factories = {};

  static void register(int typeId, MessageFactory factory) {
    _factories[typeId] = factory;
  }

  static NetMessage? deserialize(Uint8List bytes) {
    if (bytes.isEmpty) return null;
    final reader = ByteReader(bytes);
    final typeId = reader.readUint16();
    final factory = _factories[typeId];
    if (factory == null) return null;
    return factory(reader);
  }

  static Uint8List serialize(NetMessage message) {
    final writer = ByteWriter();
    writer.writeUint16(message.typeId);
    message.serialize(writer);
    return writer.toBytes();
  }
}

class PingMsg extends NetMessage {
  static const int kTypeId = 1;
  final int clientTimestampMs;

  PingMsg(this.clientTimestampMs);

  factory PingMsg.deserialize(ByteReader reader) => PingMsg(reader.readInt64());

  @override
  int get typeId => kTypeId;

  @override
  void serialize(ByteWriter writer) {
    writer.writeInt64(clientTimestampMs);
  }
}

class PongMsg extends NetMessage {
  static const int kTypeId = 2;
  final int clientTimestampMs;
  final int serverTimestampMs;

  PongMsg(this.clientTimestampMs, this.serverTimestampMs);

  factory PongMsg.deserialize(ByteReader reader) =>
      PongMsg(reader.readInt64(), reader.readInt64());

  @override
  int get typeId => kTypeId;

  @override
  void serialize(ByteWriter writer) {
    writer.writeInt64(clientTimestampMs);
    writer.writeInt64(serverTimestampMs);
  }
}

class InputMsg extends NetMessage {
  static const int kTypeId = 3;
  final int tick;
  final Uint8List inputData;

  InputMsg(this.tick, this.inputData);

  factory InputMsg.deserialize(ByteReader reader) =>
      InputMsg(reader.readUint32(), reader.readBytes());

  @override
  int get typeId => kTypeId;

  @override
  void serialize(ByteWriter writer) {
    writer.writeUint32(tick);
    writer.writeBytes(inputData);
  }
}

class EntitySnapshotMsg extends NetMessage {
  static const int kTypeId = 4;
  final int tick;
  final int netId;
  final double x;
  final double y;
  final double z;
  final double qx;
  final double qy;
  final double qz;
  final double qw;
  final double vx;
  final double vy;
  final double vz;

  EntitySnapshotMsg({
    required this.tick,
    required this.netId,
    required this.x,
    required this.y,
    required this.z,
    this.qx = 0.0,
    this.qy = 0.0,
    this.qz = 0.0,
    this.qw = 1.0,
    this.vx = 0.0,
    this.vy = 0.0,
    this.vz = 0.0,
  });

  factory EntitySnapshotMsg.deserialize(ByteReader reader) {
    return EntitySnapshotMsg(
      tick: reader.readUint32(),
      netId: reader.readUint32(),
      x: reader.readFloat32(),
      y: reader.readFloat32(),
      z: reader.readFloat32(),
      qx: reader.readFloat32(),
      qy: reader.readFloat32(),
      qz: reader.readFloat32(),
      qw: reader.readFloat32(),
      vx: reader.readFloat32(),
      vy: reader.readFloat32(),
      vz: reader.readFloat32(),
    );
  }

  @override
  int get typeId => kTypeId;

  @override
  void serialize(ByteWriter writer) {
    writer.writeUint32(tick);
    writer.writeUint32(netId);
    writer.writeFloat32(x);
    writer.writeFloat32(y);
    writer.writeFloat32(z);
    writer.writeFloat32(qx);
    writer.writeFloat32(qy);
    writer.writeFloat32(qz);
    writer.writeFloat32(qw);
    writer.writeFloat32(vx);
    writer.writeFloat32(vy);
    writer.writeFloat32(vz);
  }
}

class RpcMsg extends NetMessage {
  static const int kTypeId = 5;
  final int netId;
  final String methodName;
  final Uint8List payload;

  RpcMsg({
    required this.netId,
    required this.methodName,
    required this.payload,
  });

  factory RpcMsg.deserialize(ByteReader reader) {
    return RpcMsg(
      netId: reader.readUint32(),
      methodName: reader.readString(),
      payload: reader.readBytes(),
    );
  }

  @override
  int get typeId => kTypeId;

  @override
  void serialize(ByteWriter writer) {
    writer.writeUint32(netId);
    writer.writeString(methodName);
    writer.writeBytes(payload);
  }
}
