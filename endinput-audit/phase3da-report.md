# Phase 3D-A implementation and static validation

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

Implemented on the current local branch without committing or pushing. This report
supersedes the paused status in `phase3da-watermark-blocker.md`: the user explicitly
approved storing the last successfully processed watermark in existing auxiliary
state and requiring an explicit EndInput watermark for old all-terminal state that
lacks it. The blocker document remains as the rationale for that extension.

## A. Production changes

Only two production files were modified in this phase:

- `RowAppendTableSink.CoordinatorCommittingFactory.getCoordinatorProvider` reads
  `FlinkConnectorOptions.END_INPUT_WATERMARK` from the table options and passes it
  through the coordinator Provider.
- `CommittingWriteOperatorCoordinator`:
  - Constructor/Provider overloads accept the optional final watermark; existing
    signatures remain source-compatible and delegate with no override.
  - `restoreState` opens and reads the processed-watermark auxiliary ListState.
  - Ordinary checkpoint completion and nonterminal recovery record a watermark
    only after the corresponding commit succeeds.
  - `retireAndPromote` performs global finalization when all writers are terminal,
    then releases every eligible current terminal attempt.
  - `sendCommitSuccess` allows all-terminal sends only after successful finalization
    in this instance.
  - `recover` supports zero-reporter finalization and enforces old-state compatibility.
  - Later checkpoint-completion callbacks after finalization only replay release;
    they do not create lower real-checkpoint snapshots after MAX.

Writer data preparation, mailbox waiting, ACK validation, incoming event fencing,
CoordinatorState layout/serializer and listener implementations were not changed.

## B. allTerminal trigger

The ordinary path remains collect -> commit success -> record successfully processed
watermark -> retire/promote. Only after the complete promotion loop does the coordinator
evaluate `allTerminal()` (every terminalCoveredBy entry >= 0).

If false, it follows the existing early-terminal ACK path for newly promoted writers.
If true, it enters finalization before sending any final-transition ACK. Receiving a
marker or Flink completion notification alone cannot trigger finalization ahead of
the successful Paimon ordinary commit.

## C. Finalization committable

```java
committer.combine(Long.MAX_VALUE, resolvedWatermark, Collections.emptyList())
```

The result is passed as a singleton list to existing
`filterAndCommit(..., false, true)`.

```text
identifier: MAX
file payload: empty
watermark: configured EndInput watermark, or last successfully processed watermark
```

`StoreCommitter.combine` creates a ManifestCommittable and adds files only by iterating
its input list. The finalization list is explicitly empty; no writer map, tail payload
or replayed manifest is supplied. Ordinary data keeps its real checkpoint identifier.

No new public StoreCommitter finalization API was needed. The production streaming
committer factory already uses `ignoreEmptyCommit(false)` in `FlinkWriteSink`, so an
unfiltered empty MAX can create a snapshot. This is existing factory configuration,
not a second MAX publication mechanism. The standard coordinator test helper now
uses that same empty-commit setting.

## D. Watermark semantics and approved auxiliary state

Existing Paimon semantics are preserved: Flink's terminal MAX watermark is ignored;
the optional `end-input.watermark` is used only at EndInput. Without the option, the
ordinary observed/aligned watermark is retained. No arbitrary MAX watermark is used.

New auxiliary state:

```text
name: coordinator-last-processed-watermark
representation: ListState<Long>, zero or one value
element serializer: LongSerializer.INSTANCE
storage: existing MemoryBackendStateStore / CoordinatorState.committerStates map
```

After a successful ordinary/recovery commit, `recordProcessedWatermark` stores the
maximum of the saved value and that checkpoint's aligned watermark. Thus empty
terminal contributions can retain their watermark even when no ordinary table
snapshot was created. On restore the value is loaded before recovery; multiple
values are rejected. CoordinatorState's outer layout and serializer version remain
unchanged. No ACK or finalization-success flag is persisted.

Finalization selects the explicit configured watermark when present, otherwise this
saved value. FileStoreCommitImpl's existing snapshot construction retains the maximum
with the previous snapshot watermark, so an explicit lower value does not regress
the table snapshot's watermark. Early writer completion never applies the override.

Old all-terminal state without the auxiliary value fails with:
`Restored all-terminal state lacks watermark; configure end-input.watermark`.
With the explicit option it can finalize. Zero-reporter recovery does not invent a
MIN ordinary watermark and save it as though it had been observed. The absence stays
explicit when the old state had no ordinary watermark. Old nonterminal state retains
ordinary writer-replay behavior.

## E. Finalization success path

```text
allTerminal
  -> construct empty MAX with resolved watermark
  -> filterAndCommit returns successfully, including synchronous listener callbacks
  -> globalFinalizationCompleted = true (runtime only)
  -> iterate all logical writers
  -> send ACK to each current eligible ready target
```

Within one coordinator instance, subsequent progression does not repeat the finalization
call. There is no persisted success boolean. The runtime flag is not set before the
entire synchronous call returns.

## F. ACK release set

After success, the release loop covers `[0, parallelism)` rather than only the newly
promoted BitSet. Each send still checks terminal coverage, the fatal fence and the
current gateway. Therefore an earlier writer whose first ACK was unavailable, lost or
unconsumed is included. Duplicate release signals remain harmless at the writer.

The existing sufficient-coverage calculation and attempt-specific gateway are reused.
The final checkpoint's release covers earlier terminal boundaries without changing
their durable terminalCoveredBy values.

## G. executionAttemptReady after success

Ready callbacks retain Phase 3C registration and replay behavior. The shared send
gate now holds only when allTerminal && !globalFinalizationCompleted. An attempt
that becomes ready after successful finalization can receive ACK immediately through
the fenced replay action. Failed/reset attempts remain invalidated as before.

## H. All-terminal recovery

The restored terminal facts clear the required reporter set as before. Common recovery
completion invokes `recover` without waiting for writer events.

For all-terminal state, recovery validates the watermark prerequisite, calls ordinary
`filterAndCommit(emptyList, true, true)` to reconcile the existing recovery path, and
then invokes the same finalization path. It does not manufacture a real-checkpoint
empty snapshot, even when forced snapshots are configured: such a snapshot could
otherwise follow an already published MAX with a lower commit identifier.

The fresh instance has finalizationCompleted=false, so MAX is reconciled again.
Existing identifier filtering avoids republishing an already committed MAX snapshot;
the existing StoreCommitter listener path still receives the original supplied token.
Success enables ready-attempt replay. This does not establish exactly-once listeners.

## I. Failure behavior

Finalization runs inside the existing commit executor's fatal-fenced callback. A
filter/commit/listener exception propagates to the first-failure latch and failJob.
The runtime success flag remains false; the release loop is not reached. Queued/new
coordinator checkpoints cannot succeed afterward.

Ordinary tail data may already be successfully committed and retired at this point;
that is intentional. Its terminal fact is not ACK or finalization authority. Recovery
uses the restored state and normal filtering, then retries finalization. No error is
swallowed, and no new listener transaction or retry framework was added.

## J. Listener and partition mark-done boundary

The empty MAX traverses existing StoreCommitter -> CommitListeners notification.
Process-time PartitionMarkDoneListener detects MAX and can finalize already tracked
pending partitions when EndInput mark-done is enabled. Early terminal writers do not
send MAX or explicitly request EndInput mark-done; ordinary configured time/watermark
policies are unchanged.

Watermark-mode PartitionMarkDoneListener currently derives partition watermarks from
file messages and returns early when none exist. Therefore an empty MAX alone does
not finalize its pending partitions, even though the table snapshot watermark is
applied. No listener correction was made this phase; this remains explicitly deferred
to Phase 3D-B, as permitted by the request.

Snapshot identifier filtering and listener/external side-effect idempotence are
different guarantees. MAX publication success followed by listener failure, tracking
reconstruction and partition-action retries remain unverified/unfinished boundaries.

## K. Tests written (not executed)

Five new methods in `CommittingWriteOperatorCoordinatorEndInputTest`:

- `testFinalizationIsDataFreeAndReleasesAllAfterSuccess`: real ordinary tail files,
  no early MAX/override, empty final token, configured final watermark, one finalization
  per instance, no duplicated rows and targets ready after success.
- `testFinalAckWaitsForGlobalFinalization`: blocks finalization after ordinary success;
  asserts no premature ACK, then releases both ready writers.
- `testFinalizationFailureFencesAckAndCheckpoint`: failing finalization sends no ACK
  and prevents a queued coordinator checkpoint from succeeding.
- `testAllTerminalRecoveryPreservesWatermarkAndFiltersMax`: snapshots auxiliary state
  after an empty terminal tail, restores without reporters, preserves watermark 200,
  replays ACK and asserts the already-published MAX snapshot is not duplicated.
- `testLegacyAllTerminalRequiresExplicitWatermark`: rejects missing old watermark and
  verifies successful recovery with an explicit override.

Updated existing coordinator tests:

- `testEarlyAckAndFinalTransitionRelease` replaces permanent final hold with ACK of
  both currently ready terminal writers, plus a replacement ready attempt.
- `testDurableEarlyAckReplayAndAllTerminalRecoveryRelease` preserves early replay and
  expects all-terminal replay after successful re-finalization.
- State fixtures now include the watermark auxiliary entry; a separate explicit
  legacy fixture covers missing state. Single-writer final snapshot assertions now
  expect MAX while checking the same rows. The helper matches production streaming
  empty-commit behavior.

`CoordinatorCommitITCase.testCoordinatorCommitEndInputFinalizesAndFinishes` now expects
job FINISHED, latest commit identifier MAX, and exactly two rows after completion.
The asymmetric `testEarlyTerminalWriterWaitsForCoordinatorCommitAck` retains its logic.
Two adjacent blank-line formatting fixes in that file do not alter test behavior.

## L. Static validation

Commands run from repository root in PowerShell:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DspotlessFiles=.*RowAppendTableSink.java,.*CommittingWriteOperatorCoordinator.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java,.*CoordinatorCommitITCase.java' spotless:apply

mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 -DskipTests '-DspotlessFiles=.*RowAppendTableSink.java,.*CommittingWriteOperatorCoordinator.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java,.*CoordinatorCommitITCase.java' test-compile

git diff --check
```

Final scoped formatting: BUILD SUCCESS (`phase3da-format-final.log`). Initial static
compilation stopped at two missing blank lines in the touched E2E file; those were
corrected. Final non-fast-build test-compile: BUILD SUCCESS, compiling two production
and two test source files (`phase3da-compile-final.log`, 2026-09-14 21:30:58 local time).
Applicable normal checkstyle/scoped Spotless checks passed. `git diff --check` passed;
Git separately notes existing LF/CRLF normalization in the untouched coordinator test.

Final inspection confirmed no per-writer MAX payload, writer-side finalization,
terminal files in CoordinatorState, persisted ACK/finalization-success flags, new
listener transaction framework, rescaling, or blocking writer ACK wait changes.

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

Compilation is not runtime verification. No claim is made that the new E2E or listener
recovery scenarios have passed; they are prepared for the separate validation phase.

## M. Remaining Phase 3D-B work

- MAX publication success followed by listener failure and recovery.
- Listener pending-tracking reconstruction across filtered commits and snapshots.
- Watermark-mode empty-MAX partition completion and partition mark-done retry semantics.
- Remaining finalization recovery edges, including partial external side effects and
  repeat finalization with already-published MAX.

Phase 3D-B was not started.
