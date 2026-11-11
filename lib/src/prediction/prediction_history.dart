class PredictionFrame<TState, TInput> {
  final int tick;
  final TInput input;
  TState state;

  PredictionFrame({
    required this.tick,
    required this.input,
    required this.state,
  });
}

class ReconciliationResult<TState> {
  final bool rolledBack;
  final int rollbackTick;
  final int replayedTicksCount;
  final TState correctedState;

  ReconciliationResult({
    required this.rolledBack,
    required this.rollbackTick,
    required this.replayedTicksCount,
    required this.correctedState,
  });
}

class PredictionHistory<TState, TInput> {
  final int maxCapacity;
  final List<PredictionFrame<TState, TInput>> _frames = [];

  PredictionHistory({this.maxCapacity = 256});

  int get count => _frames.length;
  bool get isEmpty => _frames.isEmpty;
  PredictionFrame<TState, TInput>? get latest => _frames.lastOrNull;

  void record(int tick, TInput input, TState state) {
    _frames.add(PredictionFrame(tick: tick, input: input, state: state));
    if (_frames.length > maxCapacity) {
      _frames.removeAt(0);
    }
  }

  /// Reconciles local prediction against authoritative server state.
  /// If the predicted state at [serverTick] deviates according to [hasDiverged],
  /// rolls back to [authoritativeState] and re-simulates all subsequent inputs
  /// using [simulateStep].
  ReconciliationResult<TState>? reconcile({
    required int serverTick,
    required TState authoritativeState,
    required bool Function(TState predicted, TState authoritative) hasDiverged,
    required TState Function(TState current, TInput input, int tick) simulateStep,
  }) {
    final serverFrameIdx = _frames.indexWhere((f) => f.tick == serverTick);

    if (serverFrameIdx == -1) {
      // Server tick is too old or unknown, cannot reconcile
      return null;
    }

    final frame = _frames[serverFrameIdx];
    final diverged = hasDiverged(frame.state, authoritativeState);

    if (!diverged) {
      // Prediction matched! Prune older frames up to serverTick
      _frames.removeRange(0, serverFrameIdx);
      return ReconciliationResult(
        rolledBack: false,
        rollbackTick: serverTick,
        replayedTicksCount: 0,
        correctedState: _frames.last.state,
      );
    }

    // Divergence detected: Rollback and re-simulate!
    frame.state = authoritativeState;
    var currentState = authoritativeState;
    var replayedCount = 0;

    for (var i = serverFrameIdx + 1; i < _frames.length; i++) {
      final f = _frames[i];
      currentState = simulateStep(currentState, f.input, f.tick);
      f.state = currentState;
      replayedCount++;
    }

    // Prune frames older than server tick
    _frames.removeRange(0, serverFrameIdx);

    return ReconciliationResult(
      rolledBack: true,
      rollbackTick: serverTick,
      replayedTicksCount: replayedCount,
      correctedState: currentState,
    );
  }

  void clear() {
    _frames.clear();
  }
}
