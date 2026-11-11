import 'dart:typed_data';

typedef RpcHandler = void Function(int senderPeerId, Uint8List payload);

class RpcManager {
  final Map<String, RpcHandler> _handlers = {};

  void register(String methodName, RpcHandler handler) {
    _handlers[methodName] = handler;
  }

  void unregister(String methodName) {
    _handlers.remove(methodName);
  }

  bool dispatch(int senderPeerId, String methodName, Uint8List payload) {
    final handler = _handlers[methodName];
    if (handler != null) {
      handler(senderPeerId, payload);
      return true;
    }
    return false;
  }

  void clear() {
    _handlers.clear();
  }
}
