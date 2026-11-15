typedef StateInterpolator<T> = T Function(T from, T to, double alpha);

class Snapshot<T> {
  final int tick;
  final int timestampMs;
  final T state;

  const Snapshot({
    required this.tick,
    required this.timestampMs,
    required this.state,
  });
}

class SnapshotBuffer<T> {
  final int capacity;
  final List<Snapshot<T>> _snapshots = [];

  SnapshotBuffer({this.capacity = 128});

  int get count => _snapshots.length;
  bool get isEmpty => _snapshots.isEmpty;
  bool get isNotEmpty => _snapshots.isNotEmpty;

  Snapshot<T>? get latest => _snapshots.lastOrNull;
  Snapshot<T>? get oldest => _snapshots.firstOrNull;

  void add(Snapshot<T> snapshot) {
    if (_snapshots.isNotEmpty && snapshot.tick <= _snapshots.last.tick) {
      // Find insertion point to maintain ascending tick order
      var insertIdx = 0;
      while (insertIdx < _snapshots.length &&
          _snapshots[insertIdx].tick < snapshot.tick) {
        insertIdx++;
      }
      if (insertIdx < _snapshots.length &&
          _snapshots[insertIdx].tick == snapshot.tick) {
        // Replace existing tick snapshot
        _snapshots[insertIdx] = snapshot;
        return;
      }
      _snapshots.insert(insertIdx, snapshot);
    } else {
      _snapshots.add(snapshot);
    }

    if (_snapshots.length > capacity) {
      _snapshots.removeAt(0);
    }
  }

  Snapshot<T>? getAtTick(int tick) {
    for (final s in _snapshots) {
      if (s.tick == tick) return s;
    }
    return null;
  }

  /// Evaluates interpolated state at [targetTimestampMs] using [interpolator].
  T? sample(int targetTimestampMs, StateInterpolator<T> interpolator) {
    if (_snapshots.isEmpty) return null;
    if (_snapshots.length == 1) return _snapshots.first.state;

    // If target is older than oldest snapshot, return oldest
    if (targetTimestampMs <= _snapshots.first.timestampMs) {
      return _snapshots.first.state;
    }

    // If target is newer than latest snapshot, return latest (or could extrapolate)
    if (targetTimestampMs >= _snapshots.last.timestampMs) {
      return _snapshots.last.state;
    }

    // Find bounding pair
    for (var i = 0; i < _snapshots.length - 1; i++) {
      final s0 = _snapshots[i];
      final s1 = _snapshots[i + 1];

      if (s0.timestampMs <= targetTimestampMs &&
          targetTimestampMs <= s1.timestampMs) {
        final duration = s1.timestampMs - s0.timestampMs;
        if (duration <= 0) return s1.state;
        final alpha = (targetTimestampMs - s0.timestampMs) / duration;
        return interpolator(s0.state, s1.state, alpha.clamp(0.0, 1.0));
      }
    }

    return _snapshots.last.state;
  }

  void pruneBefore(int tick) {
    _snapshots.removeWhere((s) => s.tick < tick);
  }

  void clear() {
    _snapshots.clear();
  }
}
