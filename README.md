# voxnet

High-performance multiplayer state replication, snapshot interpolation, client-side prediction, server reconciliation (rollback), and RPC netcode for Dart.

## Features

- **Binary Serialization & Quantization**: Zero-overhead `ByteWriter`, `ByteReader` with variable-length integers (LEB128), and `FloatQuantizer` for compressing floats into bounded integer ranges.
- **Transport Abstraction**: Plug-and-play network transport with `LoopbackNetwork` and `LoopbackEndpoint` simulating realistic network conditions (artificial latency, jitter, packet loss).
- **Snapshot Interpolation**: `SnapshotBuffer<T>` with time-based sample interpolation across historical network frames for silky-smooth remote entity rendering.
- **Client Prediction & Server Rollback**: `PredictionHistory<TState, TInput>` with automatic rollback and re-simulation upon detecting authoritative server divergence.
- **Delta Compression & Dirty Bitmasks**: `DeltaCompressor` for transmitting only modified fields across network ticks.
- **RPC System**: Lightweight Remote Procedure Call registry and dispatch.
- **Zero Dependencies**: 100% pure Dart, runs seamlessly on VM, Web, and Native.

## Example

```dart
import 'package:voxnet/voxnet.dart';

void main() {
  final net = LoopbackNetwork();
  final server = net.createEndpoint(0);
  final client = net.createEndpoint(1);

  // Send message
  client.send(0, MessageRegistry.serialize(PingMsg(100)));

  // Poll
  final packets = server.poll();
  for (final pkt in packets) {
    final msg = MessageRegistry.deserialize(pkt.data);
    if (msg is PingMsg) {
      server.send(pkt.senderId, MessageRegistry.serialize(PongMsg(msg.clientTimestampMs, 105)));
    }
  }
}
```
