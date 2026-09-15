# Phase 2 implementation report

Scope: core per-writer terminal protocol and fatal failure fencing on branch
`pip30-endinput-redo-0821-mix`, starting at `5ef6dbc8f`. Phase 1's report and probe
evidence remain unchanged. No commit or push was made.

The user subsequently requested code completion without further test runs. All
test results below precede that instruction. The final downstream-emission fix
has been reviewed statically but has NOT been compiled or rerun in tests.

## A. Production changes

All production changes are in `paimon-flink/paimon-flink-common`:

- `CoordinatorCommittingRowDataStoreWriteOperator.endInput()` records logical
  input completion. `prepareSnapshotPreBarrier(K)` seals the final tail with
  `prepareCommit(true, K)` at a real checkpoint. Only successful preparation
  installs the terminal marker. Later barriers retain an empty terminal marker
  without preparing again. Records after EndInput are rejected.
- Its `snapshotState()` persists pending contributions before reporting the
  current contribution to the coordinator. The local baseline reported from
  pre-barrier, so reporting was moved to snapshotState as requested.
- Restored terminal entries are replayed once and retain the sealed-input fact;
  repeated EndInput cannot replace restored files with a new terminal payload.
- Ordinary streaming contributions still emit downstream. Terminal contributions
  are reported only through coordinator events: downstream operators have already
  received EndInput and cannot accept records from a later checkpoint.
- `CheckpointCommittables` adds one terminal boolean; its serializer advances to
  version 3.
- `WriterCommittables` retains the ordinary checkpoint map and derives terminal
  candidates from entries through the checkpoint being committed.
- `CommittingWriteOperatorCoordinator` adds instance-local terminal coverage and
  a first-failure guard. Collection no longer removes entries. Successful ordinary
  or recovery commit is followed by retirement and terminal promotion. Later
  alignment and watermark calculation exclude covered terminal writers.

An uncovered terminal marker received during region replay fails the coordinator
instead of silently dropping its tail; it requires global recovery. This does
not implement a new region-recovery protocol.

## B. Removed old MAX behavior

- EndInput no longer prepares or sends a data-bearing `Long.MAX_VALUE` contribution.
- Removed the dedicated MAX payload slot and its restored/repeated-EndInput
  replacement behavior from `WriterCommittables`.
- Removed coordinator aggregation of multiple writers' MAX payloads.
- MAX presence no longer grants coverage of future checkpoints.
- Removed the special per-writer MAX commit/recovery path. All terminal data uses
  ordinary real-checkpoint commit identifiers.
- Legacy per-writer MAX state/events fail explicitly rather than being interpreted
  as a new terminal marker. Shared listener/finalization infrastructure is intact.

## C. State and serializer changes

`CheckpointCommittablesSerializer` v3 appends the terminal boolean after the existing
committable list. Checkpoint id, watermark, idle flag and payload encoding remain
intact. Versions 1 and 2 decode with `terminal=false`; missing fields never imply
terminal status. Existing constructors default to false. The marker's owning
checkpoint comes from the existing contribution id, avoiding a duplicate id field.

The local baseline has no savepoint-tag-intent field and rejects savepoint auto-tag
with coordinator commit. Those existing restrictions remain unchanged.

`terminalCaptured` survives through marker-bearing writer snapshots even after
ordinary pending-file retirement. `terminalCoveredBy` is only an in-memory fact in
this coordinator instance; `CoordinatorState` and its serializer are unchanged.
Durable coverage restoration is explicitly deferred.

## D. Fatal failure fence

The existing single-thread commit executor owns `fatalFailure`. The first fatal
operation stores the failure before calling `context.failJob()`. Later normal
executor actions return without executing. Coordinator checkpoint actions check
the guard when dequeued and complete their result exceptionally if it is set.

Therefore a checkpoint already queued behind a failing commit cannot produce a
new successful recovery point, even if Flink has not yet stopped the coordinator.
Recovery failures use the same guard. Promotion and retirement are after commit,
so an exception prevents both. Executor draining for close bypasses normal-work
fencing, allowing cleanup to finish.

The fence was implemented and its single race regression passed independently
before the terminal protocol changes.

## E. Terminal promotion path

```text
writer.endInput()
  -> logical input ended
prepareSnapshotPreBarrier(K)
  -> prepareCommit(true, K) succeeds
  -> pending[K] = final ordinary payload + terminal marker
snapshotState(K)
  -> persist pending contributions
  -> send CommittableEvent(K)
coordinator.handleEventFromOperator(...)
  -> buffer contribution; terminal is only a candidate
notifyCheckpointComplete(C), C >= K
  -> align required writers
  -> collect ordinary contributions through C without removal
  -> committer.commit(...) succeeds
  -> retireAndPromote(C): terminalCoveredBy[writer] = C; retire entries <= C
checkpoint L > C
  -> covered writer excluded from required reports and active watermark inputs
```

Recovery substitutes successful `filterAndCommit(..., true, true)` before the same
retirement/promotion step. Empty writers still contribute a terminal marker and
go through the commit path before promotion.

## F. Abort inheritance

No new abort state machine was added. Pending entries are retained until completion.
If K aborts, L's snapshot includes K's original tail and marker plus L's empty
terminal contribution. A second abort carries both forward into M. Restored events
replay these original checkpoint entries, and coordinator collection through the
successful checkpoint includes the tail without changing its commit identifier.

Four durable writer regressions cover empty/non-empty writers with one/two aborts.
They check recovered entries, real data commit, no duplicated final data, pending
retirement and repeated/restored EndInput marker retention. Coordinator regressions
also cover two aborts, empty terminal promotion, failed commit and early-terminal
writer omission from a later checkpoint.

## G. Validation performed before the stop-tests instruction

Commands below run from the repository root (PowerShell). Counts overlap and must
not be added as distinct tests.

1. Independent fatal-fence regression using freshly compiled classes:

   ```powershell
   javac '@endinput-audit/phase2-compile.args'
   javac '@endinput-audit/phase2-runner-compile.args'
   java '@endinput-audit/phase2-fence-run.args'
   ```

   One test passed: `testFailedCommitFencesAlreadyQueuedCheckpoint`.
   Evidence: `phase2-fence-runtime.log`. Argfiles are local, ignored build artifacts;
   the compiler targeted Java 8. A subsequent full focused runner pass found 65
   passing tests (`java '@endinput-audit/phase2-run.args'`, `phase2-runtime.log`).

2. Focused Maven run:

   ```powershell
   mvn -o -pl paimon-flink/paimon-flink-common '-Pfast-build,flink1' '-DwildcardSuites=none' '-Dtest=CoordinatorCommittingRowDataStoreWriteOperatorTest,CommittingWriteOperatorCoordinatorTest,CommittingWriteOperatorCoordinatorEndInputTest,WriterCommittablesTest,CheckpointCommittablesSerializerTest' test
   ```

   BUILD SUCCESS; 65 tests passed. Evidence: `phase2-maven.log`.

3. Broader focused run without fast-build:

   ```powershell
   mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DwildcardSuites=none' '-Dtest=CoordinatorCommittingRowDataStoreWriteOperatorTest,CommittingWriteOperatorCoordinatorTest,CommittingWriteOperatorCoordinatorEndInputTest,WriterCommittablesTest,CheckpointCommittablesSerializerTest,CommittableEventTest,RestoredCommittableEventTest,WatermarkAlignerTest,CoordinatorCommitITCase#testCoordinatorCommitEndInput' test
   ```

   Formatting, checkstyle and normal pre-test build checks passed. Of 76 tests,
   75 passed and the bounded integration test timed out waiting for job completion
   at `CoordinatorCommitITCase:168`. BUILD FAILURE. Evidence: `phase2-final-maven.log`.
   This was not the Phase 1 stale sibling dependency failure.

4. After adding the sealing-failure/no-reprepare regression, a diagnostic run of
   the writer class plus that integration test reported all 14 writer tests passed
   (`phase2-it-diagnostic.log`). Its Windows logging URL was malformed and that
   diagnostic process was stopped; it was not a successful full Maven run.

5. The already-started corrected diagnostic run used:

   ```powershell
   mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DwildcardSuites=none' '-Dtest=CoordinatorCommitITCase#testCoordinatorCommitEndInput' '-Dlog4j.configurationFile=file:///D:/Learning/github/paimon/endinput-audit/phase2-log4j.properties' test
   ```

   One test timed out. `phase2-it-diagnostic2.log` identifies checkpoint failure
   caused by `Received element after endOfInput`: the final tail was emitted to
   the already-ended chained sink. The final code fix suppresses downstream
   terminal emission and updates its regression assertion to require empty output
   while retaining the final payload in the coordinator event.

No tests were launched after the user's stop instruction. The final downstream
fix has not been rerun; the integration outcome for the final code is unverified.
Final `git diff --check` passes. No unrelated sibling modules were changed.

## H. Remaining Phase 3 work

- CoordinatorState durable terminal restoration.
- Recovery completion / zero reporter.
- ACK production wiring and replay, using task-level mailbox yielding plus the
  Phase 1 re-entry guard.
- Global EndInput MAX/watermark/listener finalization.
- Rescale fail-fast checks.
