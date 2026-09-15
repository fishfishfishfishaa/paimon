# PR #8671 — EndInput protocol audit and runtime probes

Audited 2026-09-13, branch `pip30-endinput-redo-0821-mix`, HEAD `5ef6dbc8f`. Base: local `master`, also merge-base `a70912b5fc5b1506e0a927aca5a19f42b2627e01`. This is an audit of the checked-out code, not a claim about a newer remote PR revision.

**The current implementation does not satisfy the agreed protocol. The checkpoint-completion wait location is feasible on the tested Flink runtime, but ordinary operator-mailbox yielding is insufficient. Real-checkpoint pending-tail inheritance works, including empty writers and two aborts.**

No tracked production source was changed. Diagnostic Java, runner, and evidence are isolated in this directory. The terminal marker in the abort probe and ACK handling in the lifecycle probe are test-only experiments, not the proposed production implementation.

## A. Current implementation map

The base-to-HEAD diff contains eight files: three production classes (coordinator writer, coordinator, per-writer buffer) and five test files (CoordinatorCommitITCase, writer test, coordinator test, new coordinator EndInput test, WriterCommittablesTest). There are no serializer, listener, factory, or batch/no-checkpoint production changes in this diff. Those existing classes nevertheless determine correctness.

The base already includes PIP-30 and idle-watermark follow-up #8714. **Neither this HEAD nor its local base reports from snapshotState.** HEAD sends from emitCommittables/emitCheckpointMarker, called by prepareSnapshotPreBarrier and endInput. SnapshotState only snapshots the write state and pending map. Do not transplant a snapshot-reporting assumption from another branch. [Writer:164](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:164), [Writer:179](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:179), [Writer:196](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:196).

| Component | Current responsibility |
|---|---|
| CoordinatorCommittingRowDataStoreWriteOperator | Owns pending checkpoint ListState/map; sends serialized events; intercepts EndInput to emit MAX data and subsequent empty real-checkpoint contributions. Restores MAX into the live map, replaying ordinary entries only once. |
| PrepareCommitOperator → TableWriteOperator → RowDataStoreWriteOperator → StatelessRowDataStoreWriteOperator | Flush/prepare, write state/resource lifecycle, row writes/config refresh, and stateless unaware-bucket backend. Parent EndInput also uses MAX, but this subclass overrides it. |
| CheckpointCommittables / serializer | Checkpoint ID, file committables, watermark and idle bit. No terminal fact, writer identity, or original parallelism. Serializer v2 reads v1/v2. |
| CommittableEvent / RestoredCommittableEvent | One serialized contribution versus a serialized list plus restored checkpoint ID. Neither contains terminal-attempt fencing. |
| WriterCommittables | Real-checkpoint ordered map plus separate MAX slot and inferred endInputCoveredBy. This is received/snapshot coverage, not successful-commit terminal state. |
| CommittingWriteOperatorCoordinator | Single executor serializes event handling, checkpoint snapshots and commits; aligns every writer; optionally merges all MAX tails; global/region restore handling. |
| CoordinatorState / serializer | Commit user and serialized committer/listener state only, v1. No terminal coverage, original writer parallelism, ACK, or file payload. |
| StoreCommitter / CommitListeners | Table commit/filtering first, then metrics and original-list listener notification. Listener snapshot state is stored in the coordinator backend. |
| PartitionMarkDoneListener / Trigger | Derives partitions from file messages; detects MAX identifier; maintains independent pending partitions. Watermark mode needs a file-derived watermark before it invokes its trigger. |
| RowAppendTableSink.CoordinatorCommittingFactory / FlinkSink | Wires coordinator writer to a DiscardingSink, wraps coordinator in RecreateOnResetOperatorCoordinator. No writer event handler is registered for ACK. |
| CommitSucceededEvent | No such class/reference exists in the checked-out paimon-flink tree. ACK functionality is absent. |

The sink preconditions already enforce streaming + checkpointing + max concurrent checkpoints = 1, plus unaware-bucket append/write-only constraints. Keep them. Batch/no-checkpoint MAX code in the ordinary operator-commit path is outside this change; do not remove it merely because it uses MAX. [FlinkSink](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSink.java:378), [RowAppendTableSink](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/RowAppendTableSink.java:90).

## B. KEEP / MODIFY / REMOVE / ADD table

“State” identifies serialized/state compatibility impact. “Tests” lists reusable coverage and required expectation changes. Responsibilities marked KEEP remain necessary without adopting the old terminal assumptions.

| Class/method | Decision; current need and old assumption | Required responsibility | State / compatibility | Tests to retain or adapt |
|---|---|---|---|---|
| Writer.endInput | MODIFY. End detection needed; currently final payload = MAX. | Record EndInput intent; arrange one complete tail seal under real K. | Durable terminal metadata must survive checkpoint/restore; no durable ACK-consumed flag. | Change testEndInputEmitsFinalCommittableThenEmptyCheckpointMarker to real K and terminal marker. |
| Writer.prepareSnapshotPreBarrier / emitCommittables | MODIFY. Prepare and buffer needed; ended writer now emits only empty real markers. | On first eligible K call prepareCommit(true,K), then seal marker; never prepare new logical tail after sealing. | Contribution schema gains terminal fact. | Keep ordinary emission, watermark/idle, empty contribution, accumulation tests; add final flush/async/compaction cases. |
| Writer.snapshotState | MODIFY. Pending snapshot needed; does not currently report. | Persist tail and marker together; if report is moved here, explicitly separate pre-barrier preparation from reporting, retaining barrier-frozen metadata. | Versioned writer contribution state; preserve ordinary v1/v2 decoding. | Update event-timing assertions if moved; test state/event identity. |
| Writer.notifyCheckpointComplete | MODIFY. Ordinary map retirement needed; currently equates Flink completion with “acknowledged.” | Retain ordinary retirement only with coordinator failure fence; preserve terminal identity needed for replay/release; cooperative terminal wait. | Small terminal state only, ACK transient. | Keep ordinary prefix-clear test; add ACK A–D, nested callbacks and stale attempt tests. |
| Writer.initializeState | MODIFY. Replay needed; restores only MAX as terminal and clears ordinary live pending map. | Recover real terminal marker/tail, validate origin metadata, replay original files for recovery; restore EndInput intent without new tail generation. | v3 marker/origin metadata; reject unsupported legacy MAX terminal state explicitly. | Keep empty restored event and ordinary one-shot replay coverage; change MAX restore assertions. |
| Writer.processWatermark / processWatermarkStatus | KEEP ordinary behavior. MAX input watermark is ignored, idle is reset ACTIVE on restore. | Preserve ordinary frozen watermark/idle semantics; any global terminal watermark policy belongs to finalization. | Existing v2 idle format retained. | Keep writer watermark/idle and parity tests. |
| PrepareCommitOperator.endInput / preBarrier | KEEP for other sink paths. Parent skips preparation after its own end flag. | Scope new hook to coordinator writer; do not break ordinary operator/batch behavior. | None. | Existing ordinary EndInput/watermark ITs. |
| TableWriteOperator.snapshotState/close; RowDataStoreWriteOperator.prepareCommit; Stateless writer state | KEEP. Flush delegation/resource ownership still needed. | Call existing preparation machinery; close remains cleanup, not semantic completion. | No new underlying file-write state. | Existing write/commit harness tests. |
| CheckpointCommittables | MODIFY. Still carries a real checkpoint contribution; lacks terminal identity. | Carry terminal flag on sealing contribution K (K already exists), or equivalent small K marker; include original writer identity/parallelism for rescale rejection. | New fields; update constructors/accessors and diagnostic representation. | Keep existing contribution/event tests; empty terminal round trip. |
| CheckpointCommittablesSerializer | MODIFY. v2, compatible reader for v1/v2. | New version for marker/origin; default legacy ordinary entries to nonterminal. | Writer ListState and event payload both affected through versioned proxy. Legacy MAX must not be silently treated as ordinary real data. | Retain v1/v2 fixtures; add v3 compatibility and explicit legacy terminal rejection. |
| CommittableEvent | KEEP envelope, MODIFY payload/documentation as needed. | Continue checkpoint-ID validation; report real contributions with marker metadata. | Nested payload version changes; wrapper need not change solely for marker. | CommittableEventTest, ID mismatch tests. |
| RestoredCommittableEvent | KEEP replay distinction, MODIFY participation contract. Assumes one event from every writer. | Expect events only from writers requiring recovery participation; reconcile with already durable terminal facts. | Nested schema changes; restored checkpoint ID remains. | RestoredCommittableEventTest; add partial/zero-participant recovery. |
| WriterCommittables real map, from, ordinary merge/range reads | KEEP. Real-checkpoint accumulation and dedup still needed. | Continue ordered real-ID data aggregation. | In-memory only, fed by versioned payload. | Keep real map, invalid merge, duplicate, idle and watermark tests. |
| WriterCommittables MAX slot/constructors/mergeWith/restoreEndInput/clearEndInput | REMOVE MAX branches. Assumes MAX can wait separately, be replaced and survive ordinary retirement. | Separate received terminal marker from successful terminalCoveredBy; no committed file retention. | Replace in-memory fields; durable small fact in CoordinatorState. | Rewrite duplicate/replayed MAX tests as marker/attempt-idempotence tests. |
| Coordinator.alignCommittables / watermark helpers | MODIFY. Assumes all original slots report forever; missing watermark defaults active MIN. | Exclude a writer only after successful terminal coverage; do the same in watermark alignment so a terminal slot cannot pin MIN. | Uses new terminal facts. | Keep ordinary/idle watermark tests; add early terminal writer with later checkpoints. |
| Coordinator.notifyCheckpointComplete / recover / commitUpToCheckpoint | MODIFY. Ordinary commit path needed; global detection currently all MAX covered. | Commit real data first, then promote covered terminals; when all covered, run global finalization and ACK only after success. | Persist terminal coverage at later boundaries. | Adapt all 12 EndInput unit scenarios and ordinary recovery tests. |
| Coordinator.pollManifestCommittablesForCheckpoint | MODIFY/REMOVE MAX branch. Currently clears input before commit. | Keep real-ID combine; make retirement/promotion consistent with successful commit and fatal fencing. | No file payload added to coordinator state. | Abort collection, commit failure, retry tests. |
| Coordinator.mergeManifestCommittables / MAX watermark aggregation | REMOVE per-writer MAX merge; REUSE ONLY FOR GLOBAL FINALIZATION the ordinary combine capability. | Explicit singleton global finalization contribution, independent of final files. | No new public Committer MAX-merging API. | Empty finalization and filtered-MAX listener retries. |
| Coordinator.start / handleRestoredCommittableEvent | MODIFY. Recovery is completed only by last restored event. | Recheck completion after state load and restored events using durable covered writers + required replay participation. | Needs coverage map from coordinator state. | testRestoringAlignsBeforeRunning and recovery rejection tests retained; zero-event case added. |
| Coordinator.runInEventLoop / runCheckpointInEventLoop | ADD fatal fence, MODIFY wrappers. Exceptions only call failJob. | Latch first failure; queued business actions cannot progress; later checkpoint futures fail exceptionally. | Instance-local failure, not durable. | New failure-fence probe demonstrates current bug; assert opposite after implementation for ordinary/recovery/finalization failure. |
| executionAttemptReady / Failed / subtaskReset / handleEventFromOperator | MODIFY. Ready/failed are no-ops; attemptNumber only logged. | Track current attempt/gateway, invalidate old attempt, validate at executor execution and ACK dispatch; preserve only valid restored terminal facts on reset. | Attempt/gateway transient; durable coverage keyed by writer origin. | Stale queued events, reset while blocked, ACK replay to current attempt. |
| CoordinatorState / CoordinatorStateSerializer | MODIFY. Small state boundary needed; currently user + listener bytes only. | Add terminalCoveredBy and original parallelism; preserve committer states, omit committed file payload. | New version, retain v1 ordinary state decoding. | Version round trips, terminal later-boundary restore, rescale rejection. |
| StoreCommitter.commit / filterAndCommit / combine | KEEP table-first listener sequencing and normal combine API. | Reuse real-data reconciliation and explicit empty global contribution; optionally separate internal tracking rebuild if required, never external effects before data success. | No new public Committer method; listener state only if required by concrete retry test. | Filtered-data listener replay and failing listener tests. |
| PartitionMarkDoneListener.markDoneByWatermark | MODIFY. Empty MAX currently exits for no file-derived watermark. | Recognize terminal intent before empty-watermark return; allow existing trigger to finalize known partitions when configured. | No new serialized API necessary. | WatermarkPartitionMarkDoneTest; pending partition + empty MAX, disabled flag, retry. |
| PartitionMarkDoneTrigger / process-time listener / ReportPartStatsListener | KEEP pending maps and configured trigger. | Retry final actions from saved/rebuilt tracking; preserve opt-in semantics. | Existing listener snapshots remain; no ACK state. | PartitionMarkDoneTriggerTest, PartitionMarkDoneTest, custom action retry tests. |
| RowAppendTableSink factory / new success event | ADD event handler wiring and replayable ACK. Currently only outbound gateway. | Register handler, supply correct mailbox executor, carry attempt and sufficient terminal coverage identity; reject stale ACK. | New wire event only, no ACK-consumed state. | Lifecycle probe + production writer/factory integration and stale ACK tests. |
| Coordinator.close / waitProcessAllActions | KEEP cleanup, MODIFY interaction with fatal fence if needed. Drain is not commit-success proof. | Cleanup/barrier must still terminate after failure; release future exceptionally rather than deadlocking on skipped barrier work. | None. | Close after ordinary/finalization failure; cancellation while blocked. |

### Every coordinator-path MAX assumption

| Current path | Classification |
|---|---|
| Writer.endInput → emitCommittables(true,MAX) → prepareCommit | REMOVE per-writer MAX payload. |
| Writer.initializeState puts MAX back into pending map and marks ended | REMOVE special replay; replace with real marker replay. |
| Writer.emitCheckpointMarker after EndInput | REMOVE its “empty checkpoint proves earlier MAX” meaning; retain empty real contributions only where required. |
| WriterCommittables constructors separate MAX and assign restoredCheckpointId as coverage | REMOVE. |
| WriterCommittables.mergeWith replaces existing MAX and preserves/maxes old inferred coverage | REMOVE. Replacement is not proof that a changed/replayed tail is safely covered. |
| mergeWith sets coverage on first later real event; isEndInputCoveredBy accepts every later successful ID | REMOVE MAX-based coverage inference. Retain real checkpoint subsumption only for sealed real marker/data. |
| clearCommittablesBeforeCheckpoint retains MAX; reset/restoreEndInput recover special slot | REMOVE. |
| watermarkAt/isIdleAt fall back to MAX slot | REMOVE per-writer terminal watermark aggregation. |
| notifyCheckpointComplete/recover use allEndInputCoveredBy, append MAX watermark, switch to filterAndCommit | REMOVE data-dependent global trigger; REUSE ONLY FOR GLOBAL FINALIZATION filtering/explicit MAX identity. |
| pollManifestCommittablesForCheckpoint merges all nonempty MAX payloads and clears every MAX slot before commit | REMOVE. |
| StoreCommitter.combine / ManifestCommittable(MAX) / listener MAX detection | REUSE ONLY FOR GLOBAL FINALIZATION in this coordinator path. Existing unrelated batch/operator paths remain unchanged. |

The current code **does not** let MAX itself advance getMaxCheckpointId to infinity: a lone MAX event has maxCheckpointId = -1. A later real event establishes inferred coverage. It nevertheless assumes that this inferred coverage remains adequate for later checkpoints while retaining a MAX payload. [WriterCommittables:76](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/WriterCommittables.java:76), [WriterCommittables:113](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/WriterCommittables.java:113), [WriterCommittables:157](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/WriterCommittables.java:157).

## C. Writer lifecycle findings

Current call chain:

1. `endInput()` sets the subclass's private endOfInput, then calls `emitCommittables(true,MAX)`.
2. `prepareCommit` delegates through RowDataStoreWriteOperator → TableWriteOperator → StoreSinkWriteImpl → TableWriteImpl → underlying file-store writer. StoreSinkWriteImpl wraps each result with the supplied checkpoint ID. AppendOnlyWriter prepares by flushing, synchronizing required compaction, and draining increments. [Writer:190](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:190), [RowDataStoreWriteOperator](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/RowDataStoreWriteOperator.java:68), [StoreSinkWriteImpl](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreSinkWriteImpl.java:184). See [AppendOnlyWriter.prepareCommit](D:/Learning/github/paimon/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java:313).
3. The subclass sends the event immediately, then inserts the contribution in its map and emits downstream metrics records.
4. On a later real pre-barrier, this subclass bypasses ordinary preparation and creates an empty contribution. The parent's pre-barrier would skip work entirely after its own end flag; the subclass has a separate flag and does not call parent endInput. [Writer:179](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:179), [PrepareCommitOperator](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/PrepareCommitOperator.java:93).
5. There is no writer-specific finish override. Runtime evidence shows `finish()` occurs before the final real pre-barrier/snapshot/completion; it cannot be treated as successful task completion.
6. `snapshotState()` still runs after EndInput while the task remains active. Parent snapshots write/state, then subclass replaces pending ListState with its map values. The runtime probe observed this exact post-EndInput lifecycle.
7. `notifyCheckpointComplete(C)` clears only in-memory map entries ≤ C. It neither waits for a Paimon success response nor clears the persisted checkpoint image. MAX is larger than real C and survives indefinitely in the live map. The ListState is rebuilt at the next snapshot. [Writer:164](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:164), [Writer:171](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CoordinatorCommittingRowDataStoreWriteOperator.java:171).
8. `close()` closes underlying writer and releases memory through the parent chain. It sends no success event and proves no global finalization success. [TableWriteOperator](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/TableWriteOperator.java:133).

Answers: EndInput currently prepares MAX data: **yes**. Later real checkpoint preparation via the current lifecycle: **no**, only empty marker emission. Snapshot callbacks after EndInput: **yes while active**, not for a task restored as already finished. Pending reporting: **before snapshot in this HEAD**, plus immediate EndInput reporting.

Smallest viable hook: override only the coordinator writer's EndInput behavior to record intent; at first subsequent real pre-barrier K call the existing `prepareCommit(true,K)` through emit/buffer machinery, and attach a terminal marker only after preparation fully succeeds. Subsequent snapshots retain the sealed marker/data without generating new logical tail. If preparation/reporting is split so reporting occurs in snapshotState, preserve pre-barrier data preparation and watermark freezing; this is an explicit change from HEAD, not already-present follow-up work. No change to other PrepareCommitOperator users is required.

The abort diagnostic used this real-ID preparation hook; a later preparation produced an empty CommitMessage envelope for a previously active writer. That contains no new files, but demonstrates why “no logical tail after seal” must not be implemented as an assertion that every later Java list is empty.

## D. Coordinator ordering findings

start, handleEventFromOperator, notifyCheckpointComplete, subtaskReset and the wait/drain barrier enqueue through runInEventLoop. checkpointCoordinator uses a second wrapper on the **same single-thread executor**. recover is called within restored-event work, rather than on an independent executor. resetToCheckpoint is a pre-start scheduler operation; executionAttemptReady/Failed currently do nothing. [Coordinator:125](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:125), [Coordinator:165](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:165), [Coordinator:190](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:190), [Coordinator:286](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:286), [Coordinator:383](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:383), [Coordinator:583](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:583).

Executor FIFO preserves submission order. Scheduler callbacks/event delivery are framework-ordered at their entry, but there is no independent checkpoint-ID sorting in the executor. Cross-channel claims must not be inferred merely from FIFO: the required contribution must actually have arrived before completion handling. The runtime probe reported the sealed event from snapshotState before coordinator completion.

**Yes, checkpointCoordinator(L), queued after commit(K), can succeed after commit(K) throws.** runInEventLoop catches Throwable and calls context.failJob; it never changes RUNNING or latches failure. runCheckpointInEventLoop likewise has no guard. Executor service continues queued work. This was directly reproduced, rather than inferred:

```text
FENCE-PROBE commit(1) failed; checkpoint(2) succeeded; state=RUNNING
```

Evidence: [current-tests-runtime.log](D:/Learning/github/paimon/endinput-audit/current-tests-runtime.log), [CoordinatorFailureFenceProbeTest.java](D:/Learning/github/paimon/endinput-audit/CoordinatorFailureFenceProbeTest.java). Test failure reporting deliberately records the fatal cause without throwing, matching the relevant failJob-return scenario.

Collection already retires state before success: pollManifestCommittablesForCheckpoint clears ordinary ranges, optionally clears MAX slots, and watermark alignment advances before CommitAction runs. A failed commit may therefore leave both retired data and advanced watermark state in this instance. [Coordinator:417](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:417), [Coordinator:491](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:491), [Coordinator:520](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:520).

The smallest fence belongs at the common execution boundary, covering both wrappers and all business work, with first-failure recording before failJob. A newer checkpoint future must be completed exceptionally even if its action is skipped. Cleanup/drain futures need explicit completion semantics; blindly skipping waitProcessAllActions would deadlock close. The fence must cover failures from ordinary commit, recovery and finalization/listeners. Moving retirement after success is clearer; if ordinary retirement remains eager, the failed instance must be permanently unable to advance or serialize success.

close enqueues a drain, waits, sets CLOSED, shuts down executor, and closes committer. A blocking commit can delay it indefinitely; successful draining means queued actions returned, including caught exceptions, not that commits succeeded. [Coordinator:150](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:150).

## E. Recovery findings

Global recovery uses RecreateOnResetOperatorCoordinator, so a fresh inner coordinator is restored before start. It restores commitUser and listener bytes, initializes the committer as restored, and stays RESTORING. Expected participation is an array of **context.currentParallelism()** slots; there is no stored original participant set. Each restored event creates WriterCommittables with maxCheckpointId = event.restoredCheckpointId, including an empty event. Recovery runs only when a restored event causes all slots to align, and RUNNING follows successful recover. [Coordinator:271](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:271), [Coordinator:325](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:325), [Coordinator:364](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:364).

Consequences:

- With unchanged parallelism it requires one restored event from every original writer, including empty writers.
- A writer restored as finished does not execute initializeState to send that event. Current code has no finished-writer exemption.
- Zero restored events cannot complete recovery: start never tries an all-terminal completion condition, and completion is only called from the event handler.
- Existing EndInput recovery unit tests manually supply every event; they do not establish correct behavior for finished tasks omitted by Flink.
- A natural tryCompleteRecovery check belongs after restored coordinator state/committer initialization and after each valid restored contribution. It must regard durable covered writers as satisfied and wait for remaining required writer replay, without querying ExecutionGraph/CheckpointPlan.

Region recovery retains the coordinator instance. subtaskReset checks for invalid ordinary entries strictly before the restore checkpoint, then resets that writer's buffers and MAX. In RUNNING, a restored event discards ordinary replay and restores only MAX. This relies on ordinary recovery ownership already enforced by coordinator checkpoint ordering; it cannot be generalized to terminal facts without preserving their successful coverage. [Coordinator:286](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:286), [Coordinator:325](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/coordinator/CommittingWriteOperatorCoordinator.java:325).

Attempt fencing currently provided by this class: **none**. Ready/failed are empty; subtaskReset has no attempt argument/identity state; event attemptNumber is used for log formatting only. Framework gateway targeting is useful but does not replace checking queued work and ACK identity in the new code. Keep current-attempt bookkeeping transient; terminal coverage is the durable fact.

Test map: keep testRestoringAlignsBeforeRunning, testRejectCheckpointWhileRestoring, testRecommitOnRestoreWithoutFailover, testCheckpointAbort and region-reset tests. Adapt all EndInput recovery scenarios (partial/all restore, running writer failover, region MAX retention, already committed MAX filtering) to real-tail terminal coverage. CoordinatorCommitITCase.testCoordinatorCommitEndInput currently awaits job success **then polls for rows**; that cannot prove commit-before-success. Its assertion should inspect final data/listeners at successful job completion without post-success polling.

The inspected CoordinatorCommitITCase contains ordinary metrics/topology/watermark tests and bounded EndInput, not a real terminal global/region-failover IT. FlinkJobRecoveryITCase exercises generic savepoint/job-graph changes with checkpointing removed in setup, not this terminal coordinator protocol. Retain these as unrelated regressions; add targeted coordinator recovery ITs.

### Unsupported rescaling: both windows

A — marker only in WriterState: pending_committable_state is ordinary ListState, not a union state with writer ownership metadata. List entries can be redistributed on rescale; checkpointId alone cannot identify the originating writer. Current restore merging can reject duplicate checkpoint IDs or misattribute contributions. A terminal contribution needs original parallelism and stable originating subtask identity (and sufficient marker checkpoint identity) so a marker-bearing restore can reject a changed layout before replay/promotion. An empty terminal writer must carry the same metadata.

B — terminalCoveredBy in CoordinatorState: persist original parallelism beside the subtask-indexed coverage map; compare to currentParallelism before accepting restored terminal facts. Reject mismatches, including all-terminal/zero-event recovery where no writer could perform the check.

Current CoordinatorState v1 has neither value, and writer contribution v2 has no origin. Both formats need metadata/version work. Do not implement redistribution of terminal ownership. This audit's two-window rejection proposal is conditional on the agreed first-boundary ownership: the first covering snapshot must still contain the active writer's marker, while later finished-writer boundaries must have coordinator coverage.

## F. Listener findings

StoreCommitter.commit calls table commitMultiple, then metrics, then listener notification. filterAndCommit calls table filterAndCommitMultiple first, then passes the **original globalCommittables**, not the filtered retry list, to listeners. Thus a previously committed MAX can still retry listener effects after filtering succeeds. If table commit/recovery throws, external listeners do not run. [StoreCommitter](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreCommitter.java:110), [StoreCommitter](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreCommitter.java:118). [TableCommitImpl filtering](D:/Learning/github/paimon/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:326).

CommitListeners dispatches sequentially: partition statistics, optional mark-done, custom listeners. With partitionMarkDoneRecoverFromState=true, PartitionMarkDoneListener is included on recovery. Listener failures can occur after data success or after earlier listener side effects; neither filtering nor MAX snapshot success provides exactly-once listener effects. [CommitListeners](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/CommitListeners.java:46), [CommitListeners](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/CommitListeners.java:73).

| Input | Process-time mark done | Watermark mark done |
|---|---|---|
| Empty committable list | endInput=false; invokes normal time-based trigger, which may mark previously pending partitions by time. Does not imply global end. | No partition watermark; warns/returns. |
| Explicit ManifestCommittable(MAX), no files | endInput=true; existing trigger returns all pending partitions when mark-done-on-EndInput is enabled. | MAX is detected, but partitionWatermarks is empty, so method returns before trigger. Even a non-null MAX watermark does not fix this because it is only mapped through file messages. |

Partitions are collected from CommitMessageImpl when newFilesIncrement is nonempty, or waitCompaction applies. Watermark is each manifest's watermark assigned to eligible file-message partitions, then the maximum of these values is used for triggering. [PartitionMarkDoneListener](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneListener.java:151), [PartitionMarkDoneListener](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneListener.java:174), [PartitionMarkDoneListener](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneListener.java:198).

Pending partitions are an independent map restored/snapshotted through “mark-done-pending-partitions”. The EndInput branch returns its keys without clearing them; ordinary timed triggering removes eligible entries before external actions. Therefore finalization can be retried using restored/rebuilt pending tracking, but side effects/actions must be idempotent. Watermark restore initializes timestamps from current time, another reason not to use normal watermark eligibility to implement explicit terminal completion. [PartitionMarkDoneTrigger](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneTrigger.java:89), [PartitionMarkDoneTrigger](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneTrigger.java:127), [PartitionMarkDoneTrigger](D:/Learning/github/paimon/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/listener/PartitionMarkDoneTrigger.java:217).

**An explicit empty global MAX plus a small watermark-listener change is sufficient for the observed mark-done mechanism.** Handle terminal intent before the no-watermark return and invoke the existing trigger for configured EndInput completion; retain the early return for ordinary empty input. No generic finalization API is justified. Listener creation is still opt-in: streaming mark-done requires configured idle-to-done and partition keys; preserve those conditions.

ReportPartStatsListener likewise tracks pending partitions independently and returns the whole map on explicit MAX. Custom listeners require their own idempotence contract; do not advertise exactly-once external effects.

The current coordinator does not reliably create explicit empty MAX: it skips empty per-writer MAX payloads, and its force-snapshot fallback uses real C. Hence “all empty MAX writers automatically finalize listeners” is false. Also, an explicit empty contribution does not universally guarantee a physical MAX snapshot: table commit can ignore empty data according to existing configuration. Finalization success means the invoked table reconciliation and relevant listeners all returned successfully.

Restored original real committables can rebuild listener tracking even when table data is filtered. The existing table-first notification path already supports this for these listeners. If a specific recovery ordering needs a separate tracking step, restrict it to in-memory registration before reconciliation; defer every external action until data reconciliation succeeds.

## G. ACK runtime probe result

**SUPPORTED**, on the locally installed Flink 1.20.1 / JDK 11 runtime, for a cooperative wait in **notifyCheckpointComplete**, with task-level mailbox priority (-1), a registered event handler, and a re-entry guard.

**NOT SUPPORTED** with an unmodified operator-priority `parameters.getMailboxExecutor().yield()`: three cases reached the wait but the coordinator ping was not consumed within 10 seconds. This is a specific failed mechanism, not evidence that the agreed protocol is impossible.

Probe: [EndInputAckLifecycleProbeTest.java](D:/Learning/github/paimon/endinput-audit/EndInputAckLifecycleProbeTest.java). Final evidence: [ack-runtime.log](D:/Learning/github/paimon/endinput-audit/ack-runtime.log); initial negative evidence: [ack-operator-mailbox-runtime.log](D:/Learning/github/paimon/endinput-audit/ack-operator-mailbox-runtime.log). The job is streaming, checkpoint interval 100 ms, max concurrency 1, no restart, one task chain containing source → diagnostic writer → sink. The test uses a persistent MiniCluster extension. A pending coordinator-side CompletableFuture models the blocked commit/finalization; the task thread uses only mailbox.yield, not sleep/Future.get.

Observed callback sequence:

```text
task thread: endInput
task thread: finish
task thread: preBarrier 1 ended=true
task thread: snapshot 1 sealed=1
JM dispatcher: coordinator receives sealed
JM dispatcher: coordinator complete 1 ended=true
task thread: notifyComplete 1 sealed=1
task thread: consume ping
test thread: release blocked; job RUNNING; ping consumed
test thread: commit succeeded; send ack
task thread: consume ack
task thread: wait exits 1
task thread: close
test thread: release result=FINISHED ack=true
```

Task thread name is “Source: Sequence Source -> ack-probe -> Sink: Unnamed (1/1)#0”; coordinator callbacks use “flink-pekko.actor.default-dispatcher-N”. The test thread controls the simulated commit result, while actual Flink gateway/mailbox delivery carries the event.

| Case | Observed evidence |
|---|---|
| A: commit blocked | Wait latch reached; gateway ping consumed; job RUNNING; execution result still unavailable after a bounded 300 ms check. With one chained task, the writer task has not successfully finished. |
| B: release | Real operator ACK event consumed on task thread; wait returns; job FINISHED. |
| C: commit failure | context.failJob; job FAILED; ACK false. |
| D: cancel | Job CANCELED within bounded wait; task close observed; ACK false. |

Local Flink bytecode inspection explains the priority result: StreamTask.dispatchOperatorEvent enqueues on mainMailboxExecutor; MailboxProcessor creates that executor at priority -1. An operator-priority yield excludes those lower-priority events. Creating an executor through getMailboxExecutorFactory().createExecutor(-1) processes them. The bytecode also shows finalCheckpointCompleted is completed after the operator completion callback returns. [Bytecode evidence](D:/Learning/github/paimon/endinput-audit/streamtask-bytecode.txt).

Later checkpoints continue while the wait is active, despite max concurrent checkpoints = 1: sequential completed checkpoints can invoke nested completion callbacks. The initial task-level experiment recursively entered waits. The final probe has an inWait guard so only the outer callback waits, while nested callbacks can return; the outer callback still prevents successful task exit until ACK. This must be included and tested in production integration.

No earlier wait in finish is needed or validated: finish already occurs before the checkpoint that must seal/cover the tail, so an unconditional finish wait would create the wrong dependency. The valid observed location is checkpoint completion, not close.

Limits: this is Flink lifecycle evidence, not an implementation test of Paimon ACK semantics. Production writer integration, stale-attempt fencing, restore, long waits, and other supported Flink versions still need dedicated tests. The synthetic ACK intentionally has no durable identity logic.

## H. Abort inheritance probe result

[EndInputAbortInheritanceProbeTest.java](D:/Learning/github/paimon/endinput-audit/EndInputAbortInheritanceProbeTest.java) compiles the current writer/parents/coordinator directly from source and changes only the test writer's EndInput/pre-barrier hook to prepare the real tail at K. A separate test-only ListState<Long> “probe-terminal” stores K; this demonstrates small marker survival without pretending current CheckpointCommittables already has terminal metadata. It does not prescribe a second production state field.

Actual progression (K=101, L=102, M=103):

| Scenario | State at checkpoints/abort | Successful outcome |
|---|---|---|
| Nonempty, one abort | K snapshot: [101] with one data contribution; abort K: [101]; L snapshot: [101,102]; restore L contains K data + marker 101. | notifyComplete(102) commits data identifier 101; row “1, 1”; live pending map cleared. |
| Empty, one abort | Same keys; K file list empty; restored marker still 101. | Successful coverage callback; no data snapshot needed; pending map cleared. |
| Nonempty, two aborts | K abort retains [101]; L abort retains [101,102]; M state has [101,102,103] and marker 101. | notifyComplete(103) commits K data once under 101; pending map cleared. |
| Empty, two aborts | Same key/marker retention with empty data. | No data snapshot; pending cleared; marker restored correctly. |

All four cases passed. Evidence: [abort-runtime.log](D:/Learning/github/paimon/endinput-audit/abort-runtime.log).

1. notifyCheckpointAborted does not remove pending contributions. Actual abort callback was invoked, not merely omitted completion.
2. Later snapshots copy all pending entries, so aborted K data naturally appears in L/M state.
3. Existing poll/commitUpToCheckpoint drains ≤ successful ID; no MAX merging is required.
4. Existing contribution schema cannot distinguish empty ordinary K from terminal K. A persisted terminal fact is missing.
5. For abort inheritance, a terminal flag on K's contribution is sufficient because checkpointId already identifies K; equivalently a small retained marker with terminalDataCheckpoint=K works. The diagnostic persisted the latter. Original writer/parallelism and attempt fencing are additional identity requirements, not another file ledger.
6. Terminal identity must remain available beyond ordinary map retirement until recovery ownership has safely passed to coordinator coverage. Probe success does not itself prove that later-boundary ownership transfer.
7. This experiment deliberately changes the test hook. **Unmodified EndInput still stores final data under MAX**; the existing nine writer tests and twelve EndInput coordinator tests were separately recompiled and passed their current expectations.

The probe verifies pending-state and actual table data commit, with manually controlled operator-harness checkpoint/abort/completion callbacks. It does not claim MiniCluster checkpoint-abort injection; the MiniCluster lifecycle evidence is in G.

## I. Minimal implementation plan

Implement in this order, keeping ordinary coordinator commit behavior intact where possible:

1. **Fatal fence first.** Coordinator.runInEventLoop/runCheckpointInEventLoop and close/drain: first-failure latch and exceptional checkpoint futures; forbid business progression after any ordinary/recovery/finalization error. No serializer impact. Convert failure-fence diagnostic to a regression expecting checkpoint(2) failure; add recovery/listener failure variants and close-with-failure.
2. **Real tail + durable marker.** Coordinator writer.endInput/preBarrier/snapshotState/initializeState/notifyCheckpointComplete; CheckpointCommittables + serializer v3. Use existing flush/prepare with true,K; marker only after success; retain terminal identity across aborted checkpoints, empty writers and retirement. Add origin parallelism/subtask; reject legacy MAX terminal restore explicitly. Adapt current writer tests and promote the four abort cases into production tests; add no-new-tail/compaction assertions.
3. **Successful terminal coverage.** Coordinator.notifyCheckpointComplete/recover, WriterCommittables and alignment/watermark helpers: ordinary real-ID data commit, then promotion terminalCoveredBy=C; exclude only promoted writers. Remove all per-writer MAX slot/replacement/merge paths. CoordinatorState/serializer next version stores only coverage + original parallelism in addition to existing user/listener bytes. Test K abort/L success, partial terminal writers, first versus later checkpoint ownership and failure before promotion.
4. **Recovery participation + attempts.** Coordinator.start/restoreState/restored handler/reset/attempt callbacks/event handler: use existing recovery flow with completion recheck after initialization and each valid replay; durable terminals satisfy participation, including zero remaining events. Add current-attempt gateway registry and executor-time validation. Persist no gateway/ACK state. Add global/region recovery ITs for a finished writer, all-terminal restore, stale event/ACK, and both rescale rejection windows.
5. **Global finalization and listener retry.** Coordinator after all durable coverage: create explicit empty global MAX and invoke existing filtering/listener path, after ordinary commit success. PartitionMarkDoneListener.markDoneByWatermark handles terminal intent without requiring new file-derived watermarks. No public generic finalization API and no final data under MAX. Reuse existing listener state. Test empty MAX with pending partitions, filtered MAX after listener failure, partial listener side effects and configured idempotent actions.
6. **Writer release.** Add success event and writer.handleOperatorEvent; register it in RowAppendTableSink.CoordinatorCommittingFactory; wait cooperatively in eligible checkpoint completion using task-level mailbox facilities and re-entry protection. ACK contains current attempt and sufficient terminal/coverage identity and is replayable after restore. No “ACK consumed” state. Adapt the MiniCluster A–D probe to the actual writer/coordinator and assert data/listeners at successful job completion.

Keep FlinkSink preconditions. Do not add batch, no-checkpoint, concurrent-checkpoint, terminal rescale, or Flink internal graph integration support. No external ledger or generic state-machine framework is needed based on these probes.

## J. Remaining blockers

These are correctness blockers to completing the agreed protocol, not invitations to reopen its architecture:

- **Data loss:** per-writer MAX tail and replacement are still present; markers do not encode real tail sealing, including empty writer intent.
- **Unrecoverable terminal state:** no durable terminalCoveredBy/origin metadata, all-slot restored participation, and no zero-event recovery completion; marker retention across the first/later ownership boundary is not implemented.
- **Premature successful completion:** production writer has no ACK gate/event handler; close/drain is not a substitute. Operator-priority yielding alone cannot receive release events.
- **Stale release/data attribution:** production coordinator ignores attempt identity; terminal ACK and replay must be fenced to current writer identity.
- **Non-retryable or missed finalization:** all-covered global trigger is absent; empty MAX is omitted by current merge; watermark listener drops explicit empty MAX; custom external effects need an idempotent retry contract.
- **Checkpoint state past a failed commit:** experimentally confirmed missing fatal fence, combined with pre-success retirement. This can invalidate writer-side checkpoint recovery ownership.
- **Unsupported rescale accepted without detection:** both marker-only and coordinator-terminal windows lack original parallelism/ownership checks.

### Verification and reproduction

Run [run-probes.ps1](D:/Learning/github/paimon/endinput-audit/run-probes.ps1) from this checkout. It derives the dependency classpath from the baseline writer Maven report, recompiles the audited Paimon writer/parent/coordinator/state/listener classes and the selected tests with Java 8 source/target, places fresh classes first, and runs JUnit Platform. Diagnostics are intentionally outside normal production/test source roots.

Baseline Maven command:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common '-Pfast-build,flink1' '-DwildcardSuites=none' '-Dtest=CoordinatorCommittingRowDataStoreWriteOperatorTest' test
```

The initial sandbox run could not update an existing target file; the same command with filesystem access passed 9 tests. A fresh whole-module output attempt then exposed stale locally installed sibling dependencies (including missing GeographyType/DataEvolution APIs), so it is **not** counted as a successful clean module build. The focused direct compilation avoids treating stale copies of the audited classes as evidence. Other cached dependencies, including Flink jars and test utilities, remain in use; local Flink runtime prints pre-existing snapshot diagnostics.

Final focused suite: 3 ACK lifecycle tests + 4 abort tests + 1 failure-fence characterization + 9 existing writer tests + 12 existing coordinator EndInput tests = **29 tests**. See [verification.log](D:/Learning/github/paimon/endinput-audit/verification.log). This is an audit/probe validation, not final Maven/checkstyle/RAT verification of a production redesign.
