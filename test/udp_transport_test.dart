import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:voxnet/src/transport/reliable_channel.dart';
import 'package:voxnet/src/transport/transport.dart';
import 'package:voxnet/src/transport/udp_transport.dart';

void main() {
  group('ReliableChannelEndpoint & Fault Simulation', () {
    test(
      'delivers 4KB MTU-fragmented payloads and ordered streams under 10% loss + reordering',
      () async {
        final serverMetrics = NetTransportMetrics();
        final clientMetrics = NetTransportMetrics();

        final server = ReliableChannelEndpoint(
          localPeerId: 0,
          remotePeerId: 1,
          maxMtuBytes: 512,
          metrics: serverMetrics,
        );
        final client = ReliableChannelEndpoint(
          localPeerId: 1,
          remotePeerId: 0,
          maxMtuBytes: 512,
          metrics: clientMetrics,
        );

        final c2sQueue = UdpFaultQueue<int>(
          UdpFaultInjector(
            dropProbability: 0.10,
            reorderProbability: 0.25,
            minLatencyMs: 5,
            maxJitterMs: 12,
            maxReorderDelayMs: 30,
            seed: 2026,
          ),
        );
        final s2cQueue = UdpFaultQueue<int>(
          UdpFaultInjector(
            dropProbability: 0.10,
            reorderProbability: 0.25,
            minLatencyMs: 5,
            maxJitterMs: 12,
            maxReorderDelayMs: 30,
            seed: 9001,
          ),
        );

        // Queue a 4,096-byte message plus 25 ordered sequential messages.
        final bigPayload = Uint8List.fromList(
          List<int>.generate(4096, (i) => (i * 13 + 7) & 0xFF),
        );
        client.queueSend(
          bigPayload,
          channel: NetDeliveryChannel.reliableOrdered,
        );
        for (var i = 0; i < 25; i++) {
          client.queueSend(
            Uint8List.fromList(utf8.encode('msg-$i')),
            channel: NetDeliveryChannel.reliableOrdered,
            nowMs: 0,
          );
        }

        final receivedAtServer = <Uint8List>[];
        var nowMs = 0;
        while (receivedAtServer.length < 26 && nowMs < 15000) {
          for (final dg in client.collectOutgoingDatagrams(nowMs)) {
            c2sQueue.enqueue(0, dg, nowMs, metrics: clientMetrics);
          }
          for (final dg in server.collectOutgoingDatagrams(nowMs)) {
            s2cQueue.enqueue(1, dg, nowMs, metrics: serverMetrics);
          }

          for (final item in c2sQueue.drainReady(nowMs)) {
            for (final pkt in server.receiveDatagram(item.payload, nowMs)) {
              receivedAtServer.add(pkt.data);
            }
          }
          for (final item in s2cQueue.drainReady(nowMs)) {
            client.receiveDatagram(item.payload, nowMs);
          }

          nowMs += 15;
        }

        expect(receivedAtServer.length, 26);
        expect(receivedAtServer[0], equals(bigPayload));
        for (var i = 0; i < 25; i++) {
          expect(utf8.decode(receivedAtServer[i + 1]), 'msg-$i');
        }
        expect(clientMetrics.packetsRetransmitted, greaterThan(0));
      },
    );

    test('unreliableSequenced drops stale out-of-order datagrams', () {
      final server = ReliableChannelEndpoint(localPeerId: 0, remotePeerId: 1);
      final client = ReliableChannelEndpoint(localPeerId: 1, remotePeerId: 0);

      client.queueSend(
        Uint8List.fromList([1]),
        channel: NetDeliveryChannel.unreliableSequenced,
      );
      client.queueSend(
        Uint8List.fromList([2]),
        channel: NetDeliveryChannel.unreliableSequenced,
      );
      client.queueSend(
        Uint8List.fromList([3]),
        channel: NetDeliveryChannel.unreliableSequenced,
      );

      final outgoing = client.collectOutgoingDatagrams(10);
      expect(outgoing.length, 3);

      // Deliver packet 3 first, then stale packet 1, then packet 2 (both dropped).
      final d3 = server.receiveDatagram(outgoing[2], 20);
      final d1 = server.receiveDatagram(outgoing[0], 21);
      final d2 = server.receiveDatagram(outgoing[1], 22);

      expect(d3.single.data, equals([3]));
      expect(d1, isEmpty);
      expect(d2, isEmpty);
    });

    test('AIMD congestion window cuts on timeout and grows on ACKs', () {
      final metrics = NetTransportMetrics();
      final endpoint = ReliableChannelEndpoint(
        localPeerId: 1,
        remotePeerId: 0,
        initialCongestionWindow: 8,
        minCongestionWindow: 2,
        maxCongestionWindow: 32,
        metrics: metrics,
      );

      for (var i = 0; i < 6; i++) {
        endpoint.queueSend(Uint8List.fromList([i]));
      }
      final firstSend = endpoint.collectOutgoingDatagrams(0);
      expect(firstSend.length, 6);
      expect(endpoint.inFlightCount, 6);

      // Simulate RTO expiry without ACK -> congestionWindow halves from 8 to 4.
      final retransmit = endpoint.collectOutgoingDatagrams(500);
      expect(retransmit.length, 6);
      expect(endpoint.congestionWindow, 4);
      expect(metrics.packetsRetransmitted, 6);

      // Acknowledge all 6 packets -> congestionWindow recovers upward.
      endpoint.receiveFrame(
        ReliableChannelFrame(
          type: ReliableChannelFrame.typeAckOnly,
          channel: NetDeliveryChannel.unreliable,
          senderId: 0,
          sequence: 0,
          ack: 6,
          ackBits: 0x1F,
          messageSeq: 0,
          fragmentIndex: 0,
          fragmentCount: 0,
          payload: Uint8List(0),
        ),
        520,
      );
      expect(endpoint.inFlightCount, 0);
      expect(endpoint.congestionWindow, 10);
    });
  });

  group('UdpNetTransport over real loopback RawDatagramSocket', () {
    test(
      'transfers ordered large payloads reliably under 10% simulated drop & jitter',
      () async {
        final server = await UdpNetTransport.bind(
          InternetAddress.loopbackIPv4,
          0,
          localPeerId: 0,
          authOptions: const NetTransportAuthOptions(bearerToken: 'secret-tok'),
          maxMtuBytes: 512,
        );
        final client = await UdpNetTransport.connect(
          InternetAddress.loopbackIPv4,
          server.port,
          localPeerId: 11,
          remotePeerId: 0,
          authOptions: const NetTransportAuthOptions(bearerToken: 'secret-tok'),
          maxMtuBytes: 512,
          faultInjector: UdpFaultInjector(
            dropProbability: 0.10,
            reorderProbability: 0.20,
            minLatencyMs: 2,
            maxJitterMs: 8,
            maxReorderDelayMs: 12,
            seed: 777,
          ),
        );

        addTearDown(() async {
          await client.closeAsync();
          await server.closeAsync();
        });

        // Drive handshake over loopback socket.
        final deadline = DateTime.now().add(const Duration(seconds: 4));
        while (DateTime.now().isBefore(deadline)) {
          server.poll();
          client.poll();
          if (client.connectionState == NetConnectionState.connected &&
              server.connectedPeerIds.contains(11)) {
            break;
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        expect(client.connectionState, NetConnectionState.connected);
        expect(server.connectedPeerIds, contains(11));

        final largeSnap = Uint8List.fromList(
          List<int>.generate(3200, (i) => (i * 7 + 3) & 0xFF),
        );
        client.sendWithChannel(
          0,
          largeSnap,
          channel: NetDeliveryChannel.reliableOrdered,
        );
        for (var i = 0; i < 8; i++) {
          client.sendWithChannel(
            0,
            Uint8List.fromList(utf8.encode('pkt-$i')),
            channel: NetDeliveryChannel.reliableOrdered,
          );
        }

        final serverInbox = <NetPacket>[];
        final endAt = DateTime.now().add(const Duration(seconds: 6));
        while (serverInbox.length < 9 && DateTime.now().isBefore(endAt)) {
          serverInbox.addAll(server.poll());
          client.poll();
          await Future<void>.delayed(const Duration(milliseconds: 8));
        }

        expect(serverInbox.length, 9);
        expect(serverInbox.first.data, equals(largeSnap));
        for (var i = 0; i < 8; i++) {
          expect(utf8.decode(serverInbox[i + 1].data), 'pkt-$i');
        }
      },
    );

    test('rejects client handshake with wrong bearerToken', () async {
      final server = await UdpNetTransport.bind(
        InternetAddress.loopbackIPv4,
        0,
        localPeerId: 0,
        authOptions: const NetTransportAuthOptions(bearerToken: 'valid-token'),
      );
      final badClient = await UdpNetTransport.connect(
        InternetAddress.loopbackIPv4,
        server.port,
        localPeerId: 22,
        authOptions: const NetTransportAuthOptions(bearerToken: 'wrong-token'),
      );

      addTearDown(() async {
        await badClient.closeAsync();
        await server.closeAsync();
      });

      for (var i = 0; i < 10; i++) {
        server.poll();
        badClient.poll();
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }

      expect(server.metrics.authFailures, greaterThanOrEqualTo(1));
      expect(server.connectedPeerIds, isEmpty);
      expect(badClient.connectionState, isNot(NetConnectionState.connected));
    });
  });
}
