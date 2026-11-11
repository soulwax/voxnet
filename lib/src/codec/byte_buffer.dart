import 'dart:convert';
import 'dart:typed_data';

/// High-efficiency binary serialization writer.
class ByteWriter {
  final BytesBuilder _builder = BytesBuilder(copy: false);

  void writeUint8(int value) => _builder.addByte(value & 0xFF);

  void writeInt8(int value) => _builder.addByte(value & 0xFF);

  void writeUint16(int value) {
    final b = Uint8List(2);
    ByteData.view(b.buffer).setUint16(0, value, Endian.little);
    _builder.add(b);
  }

  void writeInt16(int value) {
    final b = Uint8List(2);
    ByteData.view(b.buffer).setInt16(0, value, Endian.little);
    _builder.add(b);
  }

  void writeUint32(int value) {
    final b = Uint8List(4);
    ByteData.view(b.buffer).setUint32(0, value, Endian.little);
    _builder.add(b);
  }

  void writeInt32(int value) {
    final b = Uint8List(4);
    ByteData.view(b.buffer).setInt32(0, value, Endian.little);
    _builder.add(b);
  }

  void writeInt64(int value) {
    final b = Uint8List(8);
    ByteData.view(b.buffer).setInt64(0, value, Endian.little);
    _builder.add(b);
  }

  void writeFloat32(double value) {
    final b = Uint8List(4);
    ByteData.view(b.buffer).setFloat32(0, value, Endian.little);
    _builder.add(b);
  }

  void writeFloat64(double value) {
    final b = Uint8List(8);
    ByteData.view(b.buffer).setFloat64(0, value, Endian.little);
    _builder.add(b);
  }

  void writeBool(bool value) => writeUint8(value ? 1 : 0);

  /// Writes a variable-length unsigned integer (LEB128).
  void writeVarInt(int value) {
    var v = value;
    while (v >= 0x80) {
      _builder.addByte((v & 0x7F) | 0x80);
      v >>= 7;
    }
    _builder.addByte(v & 0x7F);
  }

  void writeString(String str) {
    final bytes = utf8.encode(str);
    writeVarInt(bytes.length);
    _builder.add(bytes);
  }

  void writeBytes(Uint8List bytes) {
    writeVarInt(bytes.length);
    _builder.add(bytes);
  }

  void writeRawBytes(Uint8List bytes) {
    _builder.add(bytes);
  }

  Uint8List toBytes() => _builder.takeBytes();
}

/// Binary serialization reader.
class ByteReader {
  final Uint8List _bytes;
  final ByteData _view;
  int _offset = 0;

  ByteReader(this._bytes) : _view = ByteData.view(_bytes.buffer, _bytes.offsetInBytes, _bytes.lengthInBytes);

  int get offset => _offset;
  int get remaining => _bytes.length - _offset;
  bool get hasRemaining => _offset < _bytes.length;

  int readUint8() {
    final val = _view.getUint8(_offset);
    _offset += 1;
    return val;
  }

  int readInt8() {
    final val = _view.getInt8(_offset);
    _offset += 1;
    return val;
  }

  int readUint16() {
    final val = _view.getUint16(_offset, Endian.little);
    _offset += 2;
    return val;
  }

  int readInt16() {
    final val = _view.getInt16(_offset, Endian.little);
    _offset += 2;
    return val;
  }

  int readUint32() {
    final val = _view.getUint32(_offset, Endian.little);
    _offset += 4;
    return val;
  }

  int readInt32() {
    final val = _view.getInt32(_offset, Endian.little);
    _offset += 4;
    return val;
  }

  int readInt64() {
    final val = _view.getInt64(_offset, Endian.little);
    _offset += 8;
    return val;
  }

  double readFloat32() {
    final val = _view.getFloat32(_offset, Endian.little);
    _offset += 4;
    return val;
  }

  double readFloat64() {
    final val = _view.getFloat64(_offset, Endian.little);
    _offset += 8;
    return val;
  }

  bool readBool() => readUint8() != 0;

  /// Reads a variable-length unsigned integer (LEB128).
  int readVarInt() {
    var result = 0;
    var shift = 0;
    while (true) {
      final b = readUint8();
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return result;
  }

  String readString() {
    final len = readVarInt();
    final str = utf8.decode(_bytes.sublist(_offset, _offset + len));
    _offset += len;
    return str;
  }

  Uint8List readBytes() {
    final len = readVarInt();
    final res = Uint8List.sublistView(_bytes, _offset, _offset + len);
    _offset += len;
    return res;
  }

  Uint8List readRemainingBytes() {
    final res = Uint8List.sublistView(_bytes, _offset);
    _offset = _bytes.length;
    return res;
  }
}

/// Utility for quantizing floating point values into compressed integer ranges.
class FloatQuantizer {
  final double min;
  final double max;
  final int maxInt;

  FloatQuantizer({
    required this.min,
    required this.max,
    int bits = 16,
  }) : maxInt = (1 << bits) - 1;

  int quantize(double value) {
    final clamped = value.clamp(min, max);
    final normalized = (clamped - min) / (max - min);
    return (normalized * maxInt).round();
  }

  double dequantize(int quantized) {
    final normalized = quantized / maxInt;
    return min + normalized * (max - min);
  }
}
