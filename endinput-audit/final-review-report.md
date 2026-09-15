# Coordinator-commit EndInput: final review report

Validated 2026-09-15 on the current branch. The final implementation is described for reviewers in [`pr-design-summary.md`](pr-design-summary.md); individual protocol scenarios are mapped in [`final-scenario-matrix.md`](final-scenario-matrix.md). No commit, push or PR creation was requested or performed.

## A. Cleanup changes

Reviewed the production diff across writer, coordinator, committer, listener and state serializers. Corrected comments that implied idle handling was future work, that an all-terminal forced ordinary snapshot was expected, and an obsolete coordinator lifecycle diagram. The stable public constructors were retained. No new protocol or durable state was added.

Broader tests exposed Windows-specific fixture defects: two suites constructed `traceable://C:\...` URIs; their setup now uses `tempPath.toUri().getPath()` while retaining TraceableFileIO. One multi-table test left two stream writers open, preventing JUnit's temp-directory deletion; they are now closed. It also checked table 1's watermark twice; the second assertion now verifies that table 2, which had no submitted committable in that case, has no snapshot. The ordinary SQL IT now waits until a canceled job is terminal before its temp warehouse is deleted. These are test setup/synchronization changes, not coordinator commit behavior changes.

## B. Obsolete-code review

The former per-writer MAX slot, replacement/aggregation and restore helpers are absent from `WriterCommittables` and the coordinator writer. No further obsolete production helper was found to remove. The remaining per-writer MAX checks explicitly reject legacy coordinator payloads. The separate non-coordinator `CommitterOperator` still uses MAX and was intentionally preserved; global finalization, listener and shared MAX infrastructure are active. No temporary debug log or test-only production hook was found in the final diff.

## C. Broad regression commands and results

Normal profile, one successful run after fixture fixes (all test selectors in one invocation):

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DwildcardSuites=none' '-Dtest=CoordinatorCommittingRowDataStoreWriteOperatorTest,CommittingWriteOperatorCoordinatorTest,CommittingWriteOperatorCoordinatorEndInputTest,WriterCommittablesTest,CheckpointCommittablesSerializerTest,CoordinatorStateSerializerTest,CommittableEventTest,RestoredCommittableEventTest,WatermarkAlignerTest,PartitionMarkDoneTest,WatermarkPartitionMarkDoneTest,PartitionMarkDoneTriggerTest,CoordinatorPartitionFinalizationTest,CommitterOperatorTest,WriterOperatorTest,AutoTagForSavepointCommitterOperatorTest,CommittableSerializerTest,WrappedManifestCommittableSerializerTest,CustomCommitListenerTest,StoreMultiCommitterTest,TableWriteCoordinatorTest,CreateTagFromWatermarkActionFactoryTest' test
```

**178 tests; 0 failures, 0 errors, 0 skipped.** Log: [`final-broad-regression.log`](final-broad-regression.log). This includes all 119 previously selected focused unit tests plus ordinary committer and writer, multi-table, savepoint/tagging, listeners, serializer, region failover, abort, ACK and fatal-fence tests. Earlier broad attempts failed only at Windows URI setup or JUnit file-handle cleanup and were rerun after fixing those fixtures.

## D. MiniCluster / integration validation

Bounded lifecycle, run independently three times with the same normal-profile command, changing only the output log filename:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DwildcardSuites=none' '-Dtest=CoordinatorCommitITCase#testCoordinatorCommitEndInputFinalizesAndFinishes' test
```

| Run | Result | Log |
| --- | --- | --- |
| 1 | 1 passed; job FINISHED, MAX final identifier, two rows | [`final-bounded-run1.log`](final-bounded-run1.log) |
| 2 | 1 passed; same assertions | [`final-bounded-run2.log`](final-bounded-run2.log) |
| 3 | 1 passed; same assertions | [`final-bounded-run3.log`](final-bounded-run3.log) |

The earlier isolated 120-second timeout from the preceding implementation pass did **not** reproduce in these three independent runs, in addition to two earlier passes. Its cause remains unproven; it is not described as fixed.

The asymmetric early-terminal MiniCluster and idle-watermark parity methods passed in this four-method run; the other two methods failed on the stale dependency `GeographyType` (log [`final-integration-regression.log`](final-integration-regression.log)):

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DwildcardSuites=none' '-Dtest=CoordinatorCommitITCase#testCoordinatorCommitRemovesGlobalCommitterOperator+testCoordinatorCommitMetricsAndCommittedRows+testEarlyTerminalWriterWaitsForCoordinatorCommitAck+testIdleWatermarkParityAcrossCommitPaths' test
```

The early-terminal case was also passed in the previous final 121-test normal-profile run. It asserts W0 FINISHED, W1/job RUNNING and no MAX. The all-terminal case asserts FINISHED and MAX after final ACK.

The two ordinary SQL methods passed after running with locally rebuilt reactor dependencies, which avoided the old installed API jar; the first intermediate reactor run also exposed the canceled-job temp-file cleanup race, fixed before this successful run:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -am '-Pfast-build,flink1' '-DfailIfNoTests=false' '-DwildcardSuites=none' '-Dtest=CoordinatorCommitITCase#testCoordinatorCommitRemovesGlobalCommitterOperator+testCoordinatorCommitMetricsAndCommittedRows' test
```

**2 selected MiniCluster methods passed; 28 reactor modules succeeded.** Log: [`final-ordinary-it-reactor.log`](final-ordinary-it-reactor.log). Real MAX-publication/listener failures and direct durable all-terminal recovery are exercised with actual StoreCommitter under the coordinator harness; there is no dedicated MiniCluster restart-injection IT.

## E. Serializer and state compatibility

`CheckpointCommittablesSerializer` writes v3 (terminal flag) and tests round-trip both terminal values, old v1 default ACTIVE/nonterminal, old v2 preserved idle/default nonterminal, and unknown-version rejection (`CheckpointCommittablesSerializerTest`, 6 passed). `CoordinatorStateSerializer` writes v2 with writer parallelism and terminal coverage; v1 decodes with empty/unknown coverage, v2 mixed/all/no-terminal arrays round-trip, defensive copying and unsupported-version rejection (`CoordinatorStateSerializerTest`, 6 passed). `WriterCommittablesTest` (14 passed) checks real-checkpoint terminal candidate handling and legacy per-writer MAX rejection.

The committer/listener auxiliary map format was retained. Its existing name-only pending-partition state and the global `coordinator-last-processed-watermark` auxiliary `ListState<Long>` require no outer serializer bump. The latter is tested by durable all-terminal recovery preserving its finite watermark; missing legacy watermark with zero reporters fails fast unless `end-input.watermark` is configured (`testLegacyAllTerminalRequiresExplicitWatermark`). The unsupported saved-name watermark partition combination fails at listener construction before any action or final ACK (`CoordinatorPartitionFinalizationTest.testPendingWatermarkRecoveryFailsFastWithoutMarkAll`). No per-partition durable watermark was added.

## F. Final protocol scenario matrix

See [`final-scenario-matrix.md`](final-scenario-matrix.md): each of 23 requested rows names the production path, exact test and PASS or explicit unsupported-by-design status. Ordinary checkpoint/recovery, writer abort inheritance and restore, zero reporters, region failover/rollback, watermark alignment, savepoint tagging, fatal fencing, ACK lifecycle and listener retry are represented.

## G. Remaining explicit limitations

- Terminal coordinator state cannot be rescaled; restore fails fast rather than remapping coverage.
- Coordinator watermark-mode restore with nonempty saved pending partition names and `partition.end-input-to-done=false` fails fast because per-partition watermarks are absent. Process-time and mark-all EndInput completion remain supported. An empty pending-name state can reconstruct tracking from original ordinary replay.
- Arbitrary HTTP/custom action retry idempotency remains the action's contract outside this PR. MAX listener retry does not claim exactly-once external effects.
- No dedicated MiniCluster restart-injection test was added for zero-reporter or listener-failure recovery; the real StoreCommitter coordinator harness covers publication, repeated failure and recovery.
- The prior single bounded timeout remains a follow-up for broad regression despite five subsequent successful bounded runs (two earlier, three here).

## H. Final diff and static checks

Production diff inspected for duplicate logic, broad synchronization, unnecessary state, exception swallowing, early final ACK and external effects during pure reconstruction. Coordinator uses one commit executor; the only cross-thread ACK target map invalidates failed/replaced attempts. Partition reconstruction calls only in-memory trigger notifications. `StoreCommitter` changes publication filtering only when coordinator commit is enabled; the non-coordinator `filterAndCommit` branch and `CommitterOperator` remain unchanged.

Final normal-profile static/test compile command:

```powershell
mvn -o -pl paimon-flink/paimon-flink-common -Pflink1 '-DskipTests' test-compile
```

**BUILD SUCCESS**: Checkstyle, Spotless, Maven enforcer, main compile and test compile; log [`final-static-test-compile.log`](final-static-test-compile.log). A scoped `spotless:apply` formatted the cleanup changes before checks. `git diff --check` passed without whitespace errors. The working tree retains the existing uncommitted protocol changes and unrelated pre-existing untracked workspace artifacts; nothing was reset, committed or pushed.

## I. Reviewer-facing PR summary

[`pr-design-summary.md`](pr-design-summary.md) explains the final protocol directly: why per-writer data-bearing MAX was unsafe, real-checkpoint tail and marker, durable coverage, first/later/zero-reporter recovery, early versus final ACK, data-free global MAX, fatal fencing and listener retry, explicit support boundaries and validation. It does not use internal development-phase labels.

READY FOR PR REVIEW
