# Phase 3C static implementation report

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

Scope: production ACK wiring, cooperative writer completion waiting, attempt/coverage
validation and replay. Existing Phase 1–3B designs and evidence are preserved.
No commit or push was made. Phase 3D was not started.

## A. Production ACK wiring

Four production files in `paimon-flink/paimon-flink-common` changed this phase:

- New `sink/coordinator/CommitSucceededEvent` implements Flink `OperatorEvent`.
- `RowAppendTableSink.CoordinatorCommittingFactory.createStreamOperator` registers
  the writer with the production operator-event dispatcher under its operator ID.
- `CoordinatorCommittingRowDataStoreWriteOperator` implements `OperatorEventHandler`,
  validates release events, tracks runtime coverage, and yields the task mailbox
  during the covering checkpoint's completion callback.
- `CommittingWriteOperatorCoordinator` tracks ready gateways, invalidates failed/reset
  targets, sends ACK after successful promotion, and replays justified release signals.

No CoordinatorState, marker serializer, listener or global-finalization fields were
added or changed in this phase. ACK sent/consumed state is not persisted.

## B. ACK event format and coverage

```text
int subtask
int attemptNumber
long checkpointId
```

The constructor rejects negative targets/attempts, negative coverage and MAX.
The writer accepts an event only when terminal state is sealed, subtask and attempt
match its current task, and event coverage is at least `requiredTerminalCoverage`.
It monotonically updates `acknowledgedCoverage`; duplicates are harmless. An event
received before sealing, for another target/attempt, or with insufficient coverage
does not grant release. Unexpected event types are rejected.

For live capture, required coverage is the real checkpoint that sealed the tail.
On restore it is derived from the earliest restored terminal contribution. This
also handles snapshots containing only a later empty terminal marker after file
retirement. No new durable ACK or capture-identity field is necessary.

An ACK's coverage is the boundary through which the tail is known covered, not
necessarily the snapshot's original commit identifier. Live promotion sends C.
Replay sends `max(terminalCoveredBy[subtask], restored checkpoint boundary)`: if K's
successful terminal fact is in CoordinatorState(L), L covers that tail and can
release a writer restoring an empty marker owned by L. This does not advance the
durable terminalCoveredBy value or recommit any files.

## C. Active attempt tracking

`ackTargets` is a runtime ConcurrentHashMap from logical subtask to SubtaskGateway.

- `executionAttemptReady` validates callback subtask/attempt against the gateway,
  registers it, and queues a fenced replay check. The queued action verifies the
  gateway is still the registered target.
- `executionAttemptFailed` immediately removes only the matching attempt. A late
  failure callback for attempt 0 cannot remove attempt 1.
- `subtaskReset` immediately removes the outgoing target, then retains the existing
  Phase 3A/3B terminal rollback validation in its executor action.

Invalidation happens in the scheduler callback rather than waiting behind a slow
commit. The commit executor looks up the current target at send time. Flink's
attempt-specific gateway remains responsible for delivery fencing if an execution
fails concurrently with a send. No current attempt is inferred from incoming writer
events, and incoming restore-event participation/fencing is unchanged.

## D. Early-terminal ACK path

```text
notifyCheckpointComplete(C), or recovery reconciliation
  -> collect ordinary pending work
  -> ordinary commit / filterAndCommit succeeds
  -> retireAndPromote(C)
       establish terminal coverage and retire successfully processed entries
       collect newly promoted writer indices
  -> sendCommitSuccess(writer, C)
       reject fatal/uncovered state
       reject all-terminal state
       look up current ready gateway
       send CommitSucceededEvent(writer, current attempt, coverage)
```

The full promotion loop completes before any send, so a batch making several writers
terminal cannot accidentally release one before noticing that the batch made all
writers terminal. A candidate alone, or a checkpoint-completion notification before
Paimon commit success, never produces ACK. Failure leaves the Phase 2 fence effective.

## E. Final-transition hold

All send paths use `sendCommitSuccess`. It returns without sending whenever every
terminalCoveredBy entry is set. This covers newly promoted final writers, simultaneous
terminal promotion, new ready attempts, restore-event replay, and all-terminal global
recovery. The held set is derived from terminal coverage and current gateways; no
separate durable waiting set or finalization-success flag is introduced.

Consequently an all-terminal bounded job may intentionally remain waiting in this
intermediate phase. No integration assertion or production behavior was changed to
bypass the Phase 3D dependency.

## F. Writer completion wait

The constructor obtains exactly the Phase 1 mechanism:

```java
parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(-1)
```

`notifyCheckpointComplete(C)` checks for sealed terminal state covered by C. Before
calling the parent completion callback, it enters a guarded loop while acknowledged
coverage is insufficient, calling `taskMailbox.yield()` each iteration. A matching
ACK received earlier makes the loop empty. An event delivered during waiting is
processed cooperatively and advances acknowledged coverage.

`waitingForCommit` prevents nested completion callbacks processed during yielding
from recursively entering the wait. A finally block clears this runtime guard.
The terminal capture identity survives ordinary pending-entry retirement.

No Thread.sleep, Future.get, latch wait, monitor wait or busy polling is used on
the production writer/task thread. Latches in test sources coordinate a deliberately
blocked coordinator commit and are not part of the writer implementation.

## G. ACK replay

Replay is evaluated from:

- `start`, after restored state and the committer are initialized;
- `executionAttemptReady`, for the currently registered gateway;
- a validated restored event from an already covered terminal writer;
- successful ordinary/recovery promotion for newly terminal writers.

The ready callback can resend for an early durable-terminal writer without any old
ACK-consumed/sent state and without requiring its restore event. The restore-event
path can supply the later boundary needed by a retained empty terminal marker.
All paths share fatal and all-terminal checks. Attempt failure removes only the
runtime destination; it does not erase durable coverage.

## H. Failure and cancellation

The wait executes task-level mailbox actions, so it does not prevent Flink's event,
failure and cancellation processing. Interruption, mailbox shutdown and exceptions
from mailbox actions propagate out of the completion callback; they do not fabricate
ACK. The finally block releases only the re-entry guard. `close()` remains ordinary
resource cleanup and does not mark acknowledged coverage.

This uses the lifecycle established by Phase 1. This phase adds a test source for
mailbox failure during waiting, but does not claim fresh runtime cancellation proof.
Without ACK or Flink failure/cancellation, waiting is intentional—especially for the
final/all-terminal writers held for Phase 3D.

## I. Test sources written

Four new writer tests in `CoordinatorCommittingRowDataStoreWriteOperatorTest`:

- `testAckBeforeWaitAndDuplicateAck`: valid early ACK avoids yielding; duplicate is harmless.
- `testAckDuringTaskMailboxWaitRejectsStaleSignalsAndReentry`: insufficient coverage,
  wrong subtask/attempt, task-priority mailbox delivery, nested callback guard and release.
- `testMailboxFailureExitsAckWaitWithoutRelease`: mailbox exception propagates, does
  not fabricate success and does not leave the guard latched.
- `testAckReplayCoversLaterRestoredEmptyMarker`: old K coverage cannot release a
  writer requiring its later restored marker; replay coverage L can.

The common harness also injects an ACK before sealing to verify it cannot authorize
a future tail. Existing abort-inheritance tests now explicitly inject a unit-test
release after checking coordinator commit success; these tests isolate writer pending
state and do not make the single-writer coordinator send an incorrect final ACK.

Four new coordinator tests in `CommittingWriteOperatorCoordinatorEndInputTest`:

- `testEarlyAckAndFinalTransitionHold`: no candidate ACK, correct early coverage and
  target, final writer held, replacement final attempt also held.
- `testFailedCommitCannotSendAck`: no ACK on commit failure; later checkpoint fenced.
- `testInFlightCommitUsesOnlyReplacementAckTarget`: no ACK before commit finishes,
  invalidation while commit is blocked, replacement-only sends, late old failure safe.
- `testDurableEarlyAckReplayAndAllTerminalRecoveryHold`: replay to new early-terminal
  attempt from durable state, with all-terminal recovery held.

These eight new test methods were compiled, not executed. The production factory's
dispatcher registration was also inspected statically; the existing all-terminal
integration test was not run or altered in this phase.

## J. Static validation

Executed from the repository root in PowerShell:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DspotlessFiles=.*CommitSucceededEvent.java,.*CoordinatorCommittingRowDataStoreWriteOperator.java,.*RowAppendTableSink.java,.*CommittingWriteOperatorCoordinator.java,.*CoordinatorCommittingRowDataStoreWriteOperatorTest.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java' spotless:apply

mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 -DskipTests '-DspotlessFiles=.*CommitSucceededEvent.java,.*CoordinatorCommittingRowDataStoreWriteOperator.java,.*RowAppendTableSink.java,.*CommittingWriteOperatorCoordinator.java,.*CoordinatorCommittingRowDataStoreWriteOperatorTest.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java' test-compile

git diff --check
```

- Scoped formatting: BUILD SUCCESS (`phase3c-format.log`). The prior LF/CRLF issue
  in the writer and writer test was normalized because both are touched this phase.
  No unrelated files were formatted.
- Production compilation passed. Intermediate test compilation found missing checked
  exceptions in the blocked-commit test override; the declarations were corrected.
- Final non-fast-build test-compile: BUILD SUCCESS at 2026-09-13 23:36:04 local time
  (`phase3c-compile-final.log`). Applicable checkstyle and scoped Spotless checks passed.
- Final `git diff --check`: passed, checked again after continuation.
- A proposed additional fresh javac compilation was rejected before execution by
  automatic approval review because the account usage limit had been reached. It is
  not reported as executed or passed. Existing successful Maven compilation supplies
  the required static validation; no workaround or runtime test was used.

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

Final scope inspection found no ACK persistence, new coordinator-state fields,
per-writer MAX, global finalization, listener changes, redistribution or new incoming
recovery protocol. Runtime validation remains a separate pass, preferably using an
asymmetric early-terminal scenario while another writer remains active.

## K. Phase 3D dependency

Before final/all-terminal writers may receive ACK, Phase 3D must perform global
EndInput finalization and establish its successful completion, including the intended
data-free MAX/watermark/listener processing and retry behavior. Only then may it release
the currently ready final waiting attempts, including reconstructed attempts after
recovery. This phase does not implement that finalization or its success state.
