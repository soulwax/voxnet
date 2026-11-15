import 'dart:typed_data';

import 'reliable_channel.dart';
import 'transport.dart';

/// Web / non-IO stub matching `UdpNetTransport`'s public API so exporting
/// `voxnet.dart` compiles cleanly via `dart compile js`.
class UdpNetTransport implements NetTransport {
  @override
  final NetTransportAuthOptions authOptions;
  @override
  final NetTransportMetrics metrics = NetTransportMetrics();
  final int maxMtuBytes;
  final UdpFaultInjector? faultInjector;

  UdpNetTransport._(this.authOptions, this.maxMtuBytes, this.faultInjector);

  /// Binds a UDP server socket; throws [UnsupportedError] on non-IO platforms.
  static Future<UdpNetTransport> bind(
    dynamic host,
    int port, {
    int localPeerId = 0,
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    UdpFaultInjector? faultInjector,
    int maxMtuBytes = 1200,
  }) async {
    throw UnsupportedError(
      'UdpNetTransport requires dart:io RawDatagramSocket.',
    );
  }

  /// Connects a UDP client socket; throws [UnsupportedError] on non-IO platforms.
  static Future<UdpNetTransport> connect(
    dynamic host,
    int remotePort, {
    int localPeerId = 1,
    int remotePeerId = 0,
    NetTransportAuthOptions authOptions = const NetTransportAuthOptions(),
    UdpFaultInjector? faultInjector,
    int maxMtuBytes = 1200,
    dynamic bindHost,
  }) async {
    throw UnsupportedError(
      'UdpNetTransport requires dart:io RawDatagramSocket.',
    );
  }

  int get port => 0;
  int get peerId => 0;
  bool get isServer => false;

  @override
  NetConnectionState get connectionState => NetConnectionState.failed;

  @override
  List<int> get connectedPeerIds => const [];

  void addPeer(int peerId, Object address, int remotePort) {
    throw UnsupportedError(
      'UdpNetTransport requires dart:io RawDatagramSocket.',
    );
  }

  Future<bool> waitUntilConnected({
    Duration timeout = const Duration(seconds: 5),
  }) async => false;

  @override
  void send(int recipientId, Uint8List data) {}

  @override
  void broadcast(Uint8List data) {}

  @override
  void sendWithChannel(
    int recipientId,
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {}

  @override
  void broadcastWithChannel(
    Uint8List data, {
    NetDeliveryChannel channel = NetDeliveryChannel.reliableOrdered,
  }) {}

  @override
  List<NetPacket> poll([int? currentTimestampMs]) => const [];

  @override
  void close() {}

  Future<void> closeAsync() async {}
}
