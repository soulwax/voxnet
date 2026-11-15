import 'dart:async';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:voxnet/src/transport/websocket_transport.dart';

void main() {
  group('WebSocketReconnectPolicy', () {
    test('calculates exponential backoff and clamps to maxDelayMs', () {
      const policy = WebSocketReconnectPolicy(
        initialDelayMs: 200,
        maxDelayMs: 1500,
        backoffFactor: 2.0,
        maxAttempts: 4,
      );

      expect(policy.delayForAttempt(0), equals(200));
      expect(policy.delayForAttempt(1), equals(200));
      expect(policy.delayForAttempt(2), equals(400));
      expect(policy.delayForAttempt(3), equals(800));
      expect(policy.delayForAttempt(4), equals(1500)); // clamped
      expect(policy.delayForAttempt(5), equals(1500)); // clamped

      expect(
        policy.delayDuration(2),
        equals(const Duration(milliseconds: 400)),
      );

      expect(policy.canRetry(1), isTrue);
      expect(policy.canRetry(4), isTrue);
      expect(policy.canRetry(5), isFalse);
    });

    test('supports unlimited attempts when maxAttempts <= 0', () {
      const policy = WebSocketReconnectPolicy(
        initialDelayMs: 100,
        maxAttempts: 0,
      );
      expect(policy.canRetry(1), isTrue);
      expect(policy.canRetry(999), isTrue);
    });
  });

  group('WebSocketFrame binary envelope', () {
    test('encodes and decodes data frame with round-trip fidelity', () {
      final payload = Uint8List.fromList([10, 20, 30, 40, 50, 60, 70, 80]);
      final original = WebSocketFrame(
        type: WebSocketFrame.typeData,
        channel: NetDeliveryChannel.unreliableSequenced,
        senderId: 42,
        recipientId: 99,
        sequence: 12345,
        timestampMs: 9876543210,
        payload: payload,
      );

      final encoded = original.encode();
      expect(
        encoded.length,
        equals(WebSocketFrame.headerSize + payload.length),
      );

      final decoded = WebSocketFrame.decode(encoded);
      expect(decoded, isNotNull);
      expect(decoded!.type, equals(WebSocketFrame.typeData));
      expect(decoded.channel, equals(NetDeliveryChannel.unreliableSequenced));
      expect(decoded.senderId, equals(42));
      expect(decoded.recipientId, equals(99));
      expect(decoded.sequence, equals(12345));
      expect(decoded.timestampMs, equals(9876543210));
      expect(decoded.payload, equals(payload));
    });

    test('encodes and decodes control frame with empty payload', () {
      final frame = WebSocketFrame(
        type: WebSocketFrame.typePing,
        senderId: 1,
        recipientId: 0,
        sequence: 7,
        timestampMs: 123456,
        payload: Uint8List(0),
      );

      final encoded = frame.encode();
      expect(encoded.length, equals(WebSocketFrame.headerSize));

      final decoded = WebSocketFrame.decode(encoded);
      expect(decoded, isNotNull);
      expect(decoded!.type, equals(WebSocketFrame.typePing));
      expect(decoded.payload.isEmpty, isTrue);
    });

    test('returns null when buffer length is smaller than header size', () {
      final truncated = Uint8List(WebSocketFrame.headerSize - 1);
      expect(WebSocketFrame.decode(truncated), isNull);
    });
  });

  group('resolveWebSocketUri helper', () {
    test('preserves uri when bearer token is absent', () {
      final uri = Uri.parse('ws://localhost:8080/ws');
      const auth = NetTransportAuthOptions();
      expect(resolveWebSocketUri(uri, auth), equals(uri));
    });

    test('appends token query param when bearer token is present', () {
      final uri = Uri.parse('ws://localhost:8080/ws?room=lobby');
      const auth = NetTransportAuthOptions(bearerToken: 'secret_token_abc');
      final resolved = resolveWebSocketUri(uri, auth);

      expect(resolved.queryParameters['room'], equals('lobby'));
      expect(resolved.queryParameters['token'], equals('secret_token_abc'));
    });
  });

  group('WebSocketNetTransport IO: Client & Server Lifecycle', () {
    late WebSocketNetTransport server;
    late int serverPort;

    setUp(() async {
      server = await WebSocketNetTransport.bindServer(
        '127.0.0.1',
        0,
        serverPeerId: 0,
        pingInterval: const Duration(milliseconds: 50),
      );
      serverPort = server.port;
      expect(serverPort, greaterThan(0));
    });

    tearDown(() async {
      await server.closeAsync();
    });

    test(
      'server reports connected state and tracks client connections',
      () async {
        expect(server.connectionState, equals(NetConnectionState.connected));
        expect(server.connectedPeerIds, isEmpty);

        final client = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:$serverPort'),
          pingInterval: const Duration(milliseconds: 50),
        );

        final connected = await client.waitUntilConnected();
        expect(connected, isTrue);
        expect(client.connectionState, equals(NetConnectionState.connected));

        // Wait briefly for handshake completion on server
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(server.connectedPeerIds.length, equals(1));
        final assignedId = server.connectedPeerIds.first;
        expect(assignedId, greaterThan(0));
        expect(client.peerId, equals(assignedId));
        expect(client.connectedPeerIds, contains(server.peerId));

        await client.closeAsync();
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(server.connectedPeerIds, isEmpty);
        expect(client.connectionState, equals(NetConnectionState.disconnected));
      },
    );

    test('bidirectional message exchange with delivery channels', () async {
      final client = WebSocketNetTransport.connect(
        Uri.parse('ws://127.0.0.1:$serverPort'),
      );
      await client.waitUntilConnected();
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final clientPeerId = client.peerId;
      expect(clientPeerId, greaterThan(0));

      // Client sends to Server
      final clientData = Uint8List.fromList([1, 2, 3, 4]);
      client.sendWithChannel(
        0,
        clientData,
        channel: NetDeliveryChannel.reliableOrdered,
      );

      await Future<void>.delayed(const Duration(milliseconds: 60));
      final serverPackets = server.poll();
      expect(serverPackets.length, equals(1));
      expect(serverPackets.first.senderId, equals(clientPeerId));
      expect(
        serverPackets.first.channel,
        equals(NetDeliveryChannel.reliableOrdered),
      );
      expect(serverPackets.first.data, equals(clientData));

      // Server sends to Client
      final serverData = Uint8List.fromList([9, 8, 7]);
      server.sendWithChannel(
        clientPeerId,
        serverData,
        channel: NetDeliveryChannel.unreliable,
      );

      await Future<void>.delayed(const Duration(milliseconds: 60));
      final clientPackets = client.poll();
      expect(clientPackets.length, equals(1));
      expect(clientPackets.first.senderId, equals(server.peerId));
      expect(
        clientPackets.first.channel,
        equals(NetDeliveryChannel.unreliable),
      );
      expect(clientPackets.first.data, equals(serverData));

      await client.closeAsync();
    });

    test('server broadcast sends to all connected clients', () async {
      final client1 = WebSocketNetTransport.connect(
        Uri.parse('ws://127.0.0.1:$serverPort'),
      );
      final client2 = WebSocketNetTransport.connect(
        Uri.parse('ws://127.0.0.1:$serverPort'),
      );

      await client1.waitUntilConnected();
      await client2.waitUntilConnected();
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final broadcastData = Uint8List.fromList([42, 43, 44]);
      server.broadcast(broadcastData);

      await Future<void>.delayed(const Duration(milliseconds: 60));
      final p1 = client1.poll();
      final p2 = client2.poll();

      expect(p1.length, equals(1));
      expect(p1.first.data, equals(broadcastData));
      expect(p2.length, equals(1));
      expect(p2.first.data, equals(broadcastData));

      await client1.closeAsync();
      await client2.closeAsync();
    });

    test(
      'client broadcast relays through server to other peers and server poll',
      () async {
        final clientA = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:$serverPort'),
        );
        final clientB = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:$serverPort'),
        );

        await clientA.waitUntilConnected();
        await clientB.waitUntilConnected();
        await Future<void>.delayed(const Duration(milliseconds: 50));

        final msg = Uint8List.fromList([100, 101, 102]);
        clientA.broadcast(msg);

        await Future<void>.delayed(const Duration(milliseconds: 80));
        final serverPackets = server.poll();
        final clientBPackets = clientB.poll();

        expect(serverPackets.any((p) => p.senderId == clientA.peerId), isTrue);
        expect(clientBPackets.any((p) => p.senderId == clientA.peerId), isTrue);

        await clientA.closeAsync();
        await clientB.closeAsync();
      },
    );

    test(
      'heartbeat ping/pong measures RTT and does not leak into poll()',
      () async {
        final client = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:$serverPort'),
          pingInterval: const Duration(milliseconds: 40),
        );
        await client.waitUntilConnected();

        // Wait for at least 2 ping intervals
        await Future<void>.delayed(const Duration(milliseconds: 150));

        // Poll must NOT contain ping or pong packets
        final polled = client.poll();
        expect(polled, isEmpty);

        final serverPolled = server.poll();
        expect(serverPolled, isEmpty);

        // Metrics must record RTT samples
        expect(client.metrics.smoothedRttMs, greaterThanOrEqualTo(0.0));
        expect(server.metrics.smoothedRttMs, greaterThanOrEqualTo(0.0));

        await client.closeAsync();
      },
    );
  });

  group('WebSocketNetTransport Auth & Security', () {
    test('authorizes client with matching Bearer token in headers', () async {
      final server = await WebSocketNetTransport.bindServer(
        '127.0.0.1',
        0,
        authOptions: const NetTransportAuthOptions(
          bearerToken: 'correct_secret',
        ),
      );

      final client = WebSocketNetTransport.connect(
        Uri.parse('ws://127.0.0.1:${server.port}'),
        authOptions: const NetTransportAuthOptions(
          bearerToken: 'correct_secret',
        ),
      );

      final ok = await client.waitUntilConnected();
      expect(ok, isTrue);
      expect(client.connectionState, equals(NetConnectionState.connected));

      await client.closeAsync();
      await server.closeAsync();
    });

    test('authorizes client with query parameter token (Web style)', () async {
      final server = await WebSocketNetTransport.bindServer(
        '127.0.0.1',
        0,
        authOptions: const NetTransportAuthOptions(
          bearerToken: 'web_token_789',
        ),
      );

      final uri = resolveWebSocketUri(
        Uri.parse('ws://127.0.0.1:${server.port}'),
        const NetTransportAuthOptions(bearerToken: 'web_token_789'),
      );
      final client = WebSocketNetTransport.connect(uri);

      final ok = await client.waitUntilConnected();
      expect(ok, isTrue);
      expect(client.connectionState, equals(NetConnectionState.connected));

      await client.closeAsync();
      await server.closeAsync();
    });

    test(
      'rejects unauthorized client and tracks authFailures counter',
      () async {
        final server = await WebSocketNetTransport.bindServer(
          '127.0.0.1',
          0,
          authOptions: const NetTransportAuthOptions(
            bearerToken: 'secret_admin',
          ),
        );

        final client = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:${server.port}'),
          authOptions: const NetTransportAuthOptions(
            bearerToken: 'wrong_secret',
          ),
          reconnectPolicy: const WebSocketReconnectPolicy(
            maxAttempts: 1,
            initialDelayMs: 50,
          ),
        );

        final ok = await client.waitUntilConnected(
          timeout: const Duration(milliseconds: 300),
        );
        expect(ok, isFalse);
        expect(server.metrics.authFailures, greaterThanOrEqualTo(1));

        await client.closeAsync();
        await server.closeAsync();
      },
    );
  });

  group('WebSocketNetTransport Reconnect Backoff', () {
    test('client reconnects after unexpected server restart', () async {
      var server = await WebSocketNetTransport.bindServer('127.0.0.1', 0);
      final port = server.port;

      final client = WebSocketNetTransport.connect(
        Uri.parse('ws://127.0.0.1:$port'),
        reconnectPolicy: const WebSocketReconnectPolicy(
          initialDelayMs: 60,
          maxDelayMs: 200,
          backoffFactor: 1.5,
          maxAttempts: 5,
        ),
      );

      expect(await client.waitUntilConnected(), isTrue);

      // Stop server to simulate server crash
      await server.closeAsync();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(
        client.connectionState == NetConnectionState.reconnecting ||
            client.connectionState == NetConnectionState.failed,
        isTrue,
      );

      // Restart server on exact same port
      server = await WebSocketNetTransport.bindServer('127.0.0.1', port);

      // Client should automatically reconnect
      final reconnected = await client.waitUntilConnected(
        timeout: const Duration(seconds: 3),
      );
      expect(reconnected, isTrue);
      expect(client.connectionState, equals(NetConnectionState.connected));
      expect(client.metrics.reconnectCount, greaterThanOrEqualTo(1));

      await client.closeAsync();
      await server.closeAsync();
    });

    test(
      'transitions to failed state when max reconnect attempts exceeded',
      () async {
        final client = WebSocketNetTransport.connect(
          Uri.parse('ws://127.0.0.1:49999'), // No server listening
          reconnectPolicy: const WebSocketReconnectPolicy(
            initialDelayMs: 30,
            maxDelayMs: 50,
            maxAttempts: 2,
          ),
          connectTimeout: const Duration(milliseconds: 200),
        );

        final ok = await client.waitUntilConnected(
          timeout: const Duration(seconds: 3),
        );
        expect(ok, isFalse);
        expect(client.connectionState, equals(NetConnectionState.failed));
        expect(client.metrics.connectAttempts, greaterThanOrEqualTo(2));

        await client.closeAsync();
      },
    );
  });
}
