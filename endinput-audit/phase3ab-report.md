# Phase 3A / 3B implementation report

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

This phase implements durable coordinator terminal coverage and terminal-aware
recovery completion on the current local branch. Phase 1 evidence and the Phase 2
report are preserved. Existing uncommitted Phase 2 changes remain in the workspace.
No commit, push, JUnit execution, MiniCluster, integration test, probe or job was run.

## A. Production changes

Three production classes in `paimon-flink/paimon-flink-common` changed this phase:

- `sink/state/CoordinatorState`: adds a defensively copied `long[] terminalCoveredBy`
  and exposes its length as saved writer parallelism. The old constructor remains.
- `sink/state/CoordinatorStateSerializer`: version 2 encodes saved parallelism and
  coverage after the original fields; version 1 remains readable.
- `sink/coordinator/CommittingWriteOperatorCoordinator`:
  - `checkpointCoordinator` copies successful terminal coverage into state, within
    the existing fatal-fenced checkpoint action.
  - `restoreState` installs durable coverage and validates parallelism and coverage
    boundaries before restoring the committer backend.
  - `start` initializes the committer and actively evaluates recovery completion.
  - `handleRestoredCommittableEvent` accounts for required writers and conservatively
    ignores validated replays from already covered writers.
  - `tryCompleteRecovery` performs recovery commit before entering RUNNING.
  - `subtaskReset` rejects rollback before terminal coverage and restores required
    participation for an active writer reset during recovery.
  - `subtaskWatermarksAt` supports absent durable-terminal writers, including
    historical pending checkpoints before their coverage boundary. Those historical
    entries use the ordinary unknown/active fallback; at/after coverage the writer
    is excluded as idle. No terminal watermark finalization was added.

The writer lifecycle, CheckpointCommittables marker format, ordinary collection /
retirement ordering and Phase 2 fatal-failure guard were not changed this phase.

## B. CoordinatorState layout

```text
String commitUser                    existing
Map<String, byte[]> committerStates  existing auxiliary state
long[] terminalCoveredBy             new, indexed by logical writer
```

Array length is the saved writer parallelism, avoiding a second mutable in-memory
size field. Serialization explicitly writes that length as an int before the longs.
For example, `[5, -1]` means two writers, W0 covered by checkpoint 5 and W1 not
durably terminal. `-1` is the only unset sentinel. Empty array means legacy/unknown
parallelism with no terminal writers. Production checkpoints use current parallelism,
even when every element is unset. Constructors and getters copy the array so later
runtime promotion cannot mutate an already constructed state object.

No files, manifest committables, ACK metadata or global completion fields were added.

## C. Serializer compatibility

| Version | Decoding behavior |
| --- | --- |
| 1 | Reads the original commit user and auxiliary map; returns empty coverage and unknown parallelism (0). |
| 2 | Reads the same original fields, saved parallelism int, then one coverage long per writer. |
| Other | Rejected explicitly. |

Negative array lengths, lengths exceeding available bytes, coverage below -1 and
MAX coverage are rejected. Recovery additionally rejects coverage newer than its
restored checkpoint. No terminal facts are inferred from absent legacy fields.

## D. Recovery participation

`requiredRestoreWriters` is an executor-confined BitSet of outstanding contributions:

1. Initialize all current logical writers as required.
2. After coordinator-state decode, clear writers with durable coverage >= 0.
3. Accept an active writer's restore contribution only at the expected global
   restore checkpoint, buffer it with existing WriterCommittables logic, and clear
   its required bit.
4. An active writer reset while RESTORING re-adds its bit after resetting its buffer.

Thus the initial set is all logical writers minus durably terminal writers; it then
shrinks as required contributions arrive. Silence alone never establishes terminal
status. A legacy checkpoint requires every current writer.

Covered-writer replay is decoded and checked: its restore boundary cannot precede
coverage, and nonempty payloads cannot be newer than committed coverage. Valid replay
is ignored, never recommitted or merged into pending files, and never changes coverage
or required participation. The existing scheduler/wrapper event delivery is preserved;
no new attempt-number protocol or ACK replay was introduced. The local ready/failed
callbacks remain no-ops, as in Phase 2; this report does not claim new attempt fencing.

## E. Recovery completion

`tryCompleteRecovery()` is called from exactly two progression paths:

- `start` after coordinator state installation and successful committer initialization.
- `handleRestoredCommittableEvent` after buffering a required writer's contribution.

It requires RESTORING, `recoveryInitialized`, and an empty outstanding writer set.
It then calls `recover(restoredCheckpointId)`, including ordinary collection,
`filterAndCommit`, and successful retirement/promotion, before entering RUNNING.

Both callers run inside the existing fatal-fenced executor. A recovery exception
leaves the coordinator out of RUNNING and prevents later coordinator checkpoints
from succeeding. Installing saved facts does not promote a new terminal candidate.

## F. Restore K versus restore L

**First boundary K:** CoordinatorState(K) can contain only unset coverage while
WriterState(K) contains a final tail and terminal marker. That writer remains
required. Its restored contribution enters the ordinary recovery buffer;
successful recovery commit promotes the candidate through K. Coordinator state
is therefore not the sole terminal-recovery source.

**Later boundary L:** The earlier successful commit established coverage K, and
CoordinatorState(L) saved it. A fresh coordinator installs K directly, excludes
that writer from required replay, and recovers from only active writers' events.
It does not need the covered writer's old committed payload or a physical FINISHED
signal. Later coordinator checkpoints retain the restored coverage.

## G. Zero-reporter path

When all saved writers are terminal, installing state clears the entire required
set. `start` initializes the committer and immediately invokes the common condition.
Recovery reconciliation is still called with the collected ordinary batch (empty
when no replay is needed), and only success permits RUNNING. No restored writer event
is needed to trigger progress. The existing force-empty-snapshot behavior remains;
no global MAX or listener finalization is introduced.

## H. Rescale and region handling

- Saved state containing any terminal coverage is rejected when saved/current writer
  parallelism differs. Coverage is never redistributed.
- Legacy state and new state with all values unset do not add a coordinator-side
  rescale restriction; existing ordinary recovery rules still apply.
- The first-boundary window with terminal metadata only in WriterState still lacks
  saved writer identity/parallelism. Its rescale check is deferred to writer-state
  hardening, rather than adding a second marker-format change in this phase.
- `subtaskReset` is the boundary where region rollback would need terminal rebasing.
  A reset before current coverage now fails explicitly and requests global recovery;
  it cannot silently retain a future fact while continuing. Arbitrary region-lineage
  rebasing is not implemented. The same older-boundary check applies to terminal
  restore events. Resets at/after coverage retain it.

## I. Static validation

Commands were executed from the repository root in PowerShell. No test lifecycle
phase or runner was invoked.

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DspotlessFiles=.*CoordinatorState.java,.*CoordinatorStateSerializer.java,.*CoordinatorStateSerializerTest.java,.*CommittingWriteOperatorCoordinator.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java' spotless:apply

mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 -DskipTests test-compile

mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 -DskipTests '-DspotlessFiles=.*CoordinatorState.java,.*CoordinatorStateSerializer.java,.*CoordinatorStateSerializerTest.java,.*CommittingWriteOperatorCoordinator.java,.*CommittingWriteOperatorCoordinatorEndInputTest.java' test-compile

javac '@endinput-audit/phase3-compile.args'

git diff --check
```

- Formatting of this phase's five Java files succeeded.
- Initial unscoped test-compile stopped at Spotless because the Phase 2 writer and
  writer-test files contain LF/CRLF-only differences. Those files were not rewritten.
  Evidence: `phase3-test-compile.log`.
- Scoped test-compile succeeded, including production compilation, test compilation,
  checkstyle and the applicable normal pre-test lifecycle checks. Evidence:
  `phase3-test-compile-final.log`; formatting: `phase3-format-final.log`.
- Fresh focused javac compilation succeeded with source/target 8, including Phase 2
  writer/coordinator/fatal-fence tests and the new serializer test. No runner source
  was included or executed. Evidence: `phase3-javac.log`. JDK 11 reports the expected
  missing Java 8 bootstrap-classpath warning and unchecked-operation notes; this is
  not a full JDK 8 API compatibility certification.
- `git diff --check` passes; Git separately warns about the same pre-existing
  Phase 2 LF/CRLF normalization. No unrelated module/dependency installation changed.
- Final diff inspection found no terminal payloads in CoordinatorState, ACK wiring,
  new listener APIs, global finalization, per-writer MAX, rescale redistribution or
  internal scheduler/checkpoint APIs.

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE. Compilation is not runtime proof.

## J. Test sources added / modified for later execution

New `sink/state/CoordinatorStateSerializerTest` contains six tests:

- `testMixedTerminalRoundTrip`
- `testAllTerminalRoundTrip`
- `testNoTerminalRoundTrip`
- `testVersionOneHasNoTerminalCoverage`
- `testCoverageIsDefensivelyCopied`
- `testRejectsUnsupportedVersion`

Added ten tests to `CommittingWriteOperatorCoordinatorEndInputTest`:

- `testLaterCheckpointRestoresTerminalWithoutWriterEvent`
- `testFirstTerminalBoundaryStillRequiresWriterReplay`
- `testAllTerminalRecoveryCompletesWithoutReporters`
- `testAllTerminalRecoveryFailureFencesCheckpoint`
- `testOrdinaryRecoveryRequiresEveryWriter`
- `testTerminalStateRescaleFailsRecovery`
- `testRegionResetBeforeTerminalCoverageFails`
- `testResetDuringRecoveryRequiresFreshContribution`
- `testNoTerminalStateDoesNotRejectParallelismChange`
- `testRecoveryWithPendingCheckpointBeforeTerminalCoverage`

The six Phase 2 EndInput coordinator tests remain. Existing fatal-fence regressions
in `CommittingWriteOperatorCoordinatorTest` were not weakened or deleted and were
included in fresh static compilation.

## K. Remaining Phase 3C / 3D work

- ACK production wiring and replay.
- Global EndInput finalization.
- MAX/watermark/listener retry.
- Remaining writer-state rescale hardening at the first terminal boundary.
