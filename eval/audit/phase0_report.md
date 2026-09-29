# Phase 0 — Repository Audit + Environment Baseline Report
**Project:** ChronoCortex-Saga (CCS-Agent)  
**Date:** 2026-09-30  
**Status:** Audit Completed — Baseline Established  

---

## 1. Repository Tree & File Inventory

### Current Tree Structure
```text
C:\Users\dushi\Desktop\PRISM
├── agent.py
├── eval/
│   ├── audit/
│   │   ├── phase0_report.md
│   │   └── repro_commit_write_race.py
│   └── reproduce_eval.sh
├── runtime/
│   ├── grounding_guard.py
│   └── speculative_saga.py
└── tools/
    └── fleet_tools.py
```

### Verification of Queried Files / Packages
| Target Path / Item | Status | Notes |
| :--- | :--- | :--- |
| `tools/benchmark_tools.py` | **MISSING** | Not present on disk. |
| `eval/run_fdb_eval.py` | **MISSING** | Not present on disk. |
| `web/index.html` | **MISSING** | Not present on disk. |
| `requirements.txt` | **MISSING** | Not present on disk; referenced by `reproduce_eval.sh`. |
| `README.md` | **MISSING** | Not present on disk. |
| `tests/` | **MISSING** | No unit or integration test directory exists. |
| `fdbench` package | **MISSING** | No module/package found; required by `reproduce_eval.sh` (`fdbench.runner`, `fdbench.score`). |

---

## 2. LiveKit SDK Environment & Event Binding Audit

### Installed SDK Version
Command executed:
```bash
pip show livekit-agents
```
Output:
```text
WARNING: Package(s) not found: livekit-agents
Exit Code: 1
```
`livekit-agents` (and associated plugins `livekit-plugins-deepgram`, `livekit-plugins-silero`, `livekit-plugins-cartesia`) is **not installed** in the local Python environment (Python 3.13.9).

### SDK Event Compatibility Analysis
In `agent.py` (lines 182–185), the following bindings are declared on `AgentSession`:
```python
session.on("user_speech_committed", lambda ev: ccs.on_interim_transcript(
    ev.transcript, getattr(ev, "confidence", 1.0), ev.start_time, ev.end_time,
))
session.on("agent_speech_interrupted", lambda ev: ccs.on_barge_in())
```

#### Event 1: `user_speech_committed`
- **Is it a real event?**
  - In `livekit-agents` v0.x (`VoicePipelineAgent`), `user_speech_committed` existed, but represented the **final committed turn** (passing a `ChatMessage`), **not** an interim token/partial transcript with timing metadata (`start_time`, `end_time`, `confidence`).
  - In `livekit-agents` v1.0+ (`AgentSession`), `user_speech_committed` **does not exist**.
- **Correct Event for Interim Transcripts:**
  - For `AgentSession` (v1.x+): `user_input_transcribed` (emitted on streaming transcription events, exposing `ev.transcript`, `ev.is_final`).
  - At the raw STT layer: `stt.SpeechEventType.INTERIM_TRANSCRIPT`.

#### Event 2: `agent_speech_interrupted`
- **Is it a real event?**
  - In `VoicePipelineAgent` (v0.x), `agent_speech_interrupted` was emitted when user speech interrupted playback.
  - In `AgentSession` (v1.x+), `agent_speech_interrupted` is **not a standard event**. Interruption/barge-in is managed via `TurnHandlingOptions` and signaled via `agent_state_changed`, `interrupted`, or `agent_false_interruption`.

---

## 3. Reproduction of Race Condition in `commit_write`

### Repro Script
Script path: [`eval/audit/repro_commit_write_race.py`](file:///C:/Users/dushi/Desktop/PRISM/eval/audit/repro_commit_write_race.py)

### Reproduction Mechanics
1. A mutating write is staged via `saga.stage_write("test_mutate", {"key": "val"})` at `capture_epoch = 0` (`state = ActionState.PENDING`). Note that `stage_write` does not assign `action._task`.
2. `saga.commit_write(action.action_id, slow_executor)` is started. It sets `action.state = ActionState.IN_FLIGHT` and awaits the asynchronous write executor.
3. While the executor is in-flight, `epoch_clock.advance(reason="barge_in")` is triggered.
4. `TurnEpochClock.advance()` calls `_on_epoch_advance(1)`.
5. Because `action.capture_epoch (0) < new_epoch (1)`, `_on_epoch_advance` marks `action.state = ActionState.ABORTED`.
6. When the executor returns, `commit_write` resumes after line 257. Because `commit_write` lacks a post-execution state/epoch check, lines 269–271 execute unconditionally:
   ```python
   action.result = result
   action.state = ActionState.COMMITTED
   self._chain.append(action)
   ```
7. `action.state` is overwritten from `ABORTED` to `COMMITTED`, and the superseded action is improperly appended to `_chain`.

### Exact Reproduction Output
Command: `python eval/audit/repro_commit_write_race.py`
```text
=== Reproducing commit_write race condition ===
[Init] Initial epoch: 0
[1] Staged action write-0-18e29f at epoch 0, state: PENDING
[Executor] slow_executor called with {'key': 'val'}
[2] Inside commit_write: action state is IN_FLIGHT
[3] Advancing epoch via clock.advance(reason='barge_in')...
[3] New epoch is 1. Clock current: 1
[3] Immediately after epoch advance, action state is: ABORTED
    -> Confirmed: _on_epoch_advance marked action as ABORTED.
[4] Allowing executor to finish...
[Executor] slow_executor finished work, returning result
[4] commit_write returned: {'result': 'success'}
[5] Final action state: COMMITTED
[5] Action in saga._chain: True

*** RACE CONDITION REPRODUCED SUCCESSFULLY ***
BUG CONFIRMED: Action write-0-18e29f was ABORTED due to epoch advance (0 < 1), but commit_write unconditionally overwrote ABORTED with COMMITTED and appended it to _chain!
```
**Result:** Bug reproduced deterministically.

---

## 4. `eval/reproduce_eval.sh` Execution Trace in Clean Virtual Environment

### Command Run
```bash
./eval/reproduce_eval.sh
```

### Exact Failure Trace
```text
=== ChronoCortex-Saga :: FDB-v3 Reproduction ===
GPU: 0   Results: ./results/fdb_v3_20260930_012614
[1/6] Checking environment...
Python 3.13.9
  no local GPU detected — assuming hosted-API mode
[2/6] Installing pinned dependencies...
ERROR: Could not open requirements file: [Errno 2] No such file or directory: 'requirements.txt'
```
Exit code: `1`

### Failure Point & Upstream/Downstream Gaps
1. **Primary Failure:** Step `[2/6]` fails immediately because `requirements.txt` does not exist in the repository root.
2. **Cascading Missing Requirements:**
   - **Step 3 (`FDBENCH_DIR`):** `./benchmarks/fdb-v3` does not exist on disk.
   - **Step 4 (`python3 -m agent`):** Fails because `livekit-agents` is uninstalled, and `agent.py` does not accept CLI flags `--eval-mode`, `--fdb-dir`, `--results-dir`, `--domains`, `--chain-depths`.
   - **Steps 5 & 6 (`fdbench.runner`, `fdbench.score`):** Fails because the `fdbench` package is completely absent.

---

## 5. Remaining Risks & Observations
1. **`commit_write` Concurrency Hazard:** Without an epoch/state check after `await executor(...)`, any network delay during a mutation allows barge-in invalidations to be reverted to committed, corrupting saga rollback chains.
2. **Missing `_task` tracking in `stage_write` / `commit_write`:** `_on_epoch_advance` attempts `action._task.cancel()`, but `_task` is only ever stored on speculative reads, not staged/committed writes.
3. **LiveKit SDK API Divergence:** `agent.py` mixes constructs from LiveKit Agents v0.x (`user_speech_committed`, `agent_speech_interrupted`) with v1.x class names (`AgentSession`).
4. **Benchmark & Test Isolation:** No evaluation harness (`fdbench`), dataset directory (`benchmarks/fdb-v3`), or unit tests exist.
