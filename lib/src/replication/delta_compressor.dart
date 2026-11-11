import '../codec/byte_buffer.dart';

/// Manages bitmask-based dirty tracking and delta compression for networked entities.
class DeltaCompressor {
  int _dirtyMask = 0;

  bool isDirty(int fieldIndex) => (_dirtyMask & (1 << fieldIndex)) != 0;

  void markDirty(int fieldIndex) {
    _dirtyMask |= (1 << fieldIndex);
  }

  void clearDirty() {
    _dirtyMask = 0;
  }

  int get dirtyMask => _dirtyMask;
  bool get hasChanges => _dirtyMask != 0;

  void writeMask(ByteWriter writer) {
    writer.writeVarInt(_dirtyMask);
  }

  static int readMask(ByteReader reader) {
    return reader.readVarInt();
  }

  static bool isFieldInMask(int mask, int fieldIndex) {
    return (mask & (1 << fieldIndex)) != 0;
  }
}
