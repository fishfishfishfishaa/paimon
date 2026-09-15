# Final coordinator-commit scenario matrix

`PASS` refers to the normal-profile `final-broad-regression.log` run (178/178) unless the row names a MiniCluster log. The unsupported rows pass by asserting a deliberate fail-fast. Paths are production classes/methods, and the test column names the exact test method.

| Scenario | Production path | Test | Status |
| --- | --- | --- | --- |
| Normal checkpoint commit | `CommittingWriteOperatorCoordinator.notifyCheckpointComplete`, `commitUpToCheckpoint` | `CommittingWriteOperatorCoordinatorTest.testCommitSingleSubtask`, `testCommitFanInFromMultipleSubtasks` | PASS |
| Early EndInput | writer `endInput` / `prepareSnapshotPreBarrier`; coordinator `retireAndPromote` | `CommittingWriteOperatorCoordinatorEndInputTest.testEarlyAckAndFinalTransitionRelease`; `CoordinatorCommitITCase.testEarlyTerminalWriterWaitsForCoordinatorCommitAck` | PASS |
| Empty terminal writer | writer terminal `CheckpointCommittables`; coordinator `retireAndPromote` | `CommittingWriteOperatorCoordinatorEndInputTest.testEmptyTerminalPromotesAfterCommit`, `CoordinatorCommittingRowDataStoreWriteOperatorTest.testOneAbortEmpty` | PASS |
| K abort then L success | writer `prepareSnapshotPreBarrier`, `notifyCheckpointComplete`; coordinator `hasTerminalCandidate` | `CoordinatorCommittingRowDataStoreWriteOperatorTest.testOneAbortWithTail`, `CommittingWriteOperatorCoordinatorEndInputTest.testTerminalCandidateSurvivesTwoAborts` | PASS |
| Multiple aborts | writer terminal candidate buffering | `CoordinatorCommittingRowDataStoreWriteOperatorTest.testTwoAbortsWithTail`, `testTwoAbortsEmpty` | PASS |
| Restore first terminal boundary K | coordinator `requiredRestoreWriters`, `recover` | `CommittingWriteOperatorCoordinatorEndInputTest.testFirstTerminalBoundaryStillRequiresWriterReplay`, `testRestoredRealMarkerPromotesAfterRecoveryCommit` | PASS |
| Restore later durable boundary L | coordinator `restoreState`, `requiredRestoreWriters` | `CommittingWriteOperatorCoordinatorEndInputTest.testLaterCheckpointRestoresTerminalWithoutWriterEvent` | PASS |
| Zero-reporter recovery | coordinator `tryCompleteRecovery`, all-terminal `recover` | `CommittingWriteOperatorCoordinatorEndInputTest.testAllTerminalRecoveryCompletesWithoutReporters`, `testPublishedMaxRetriesListenerFromAllTerminalState` | PASS, harness (no dedicated MiniCluster restart IT) |
| Ordinary commit failure | coordinator `notifyCheckpointComplete`, fatal `fail` | `CommittingWriteOperatorCoordinatorEndInputTest.testFailedTerminalCommitDoesNotPromoteOrRetire` | PASS |
| Recovery commit failure | coordinator `recover`, fatal `fail` | `CommittingWriteOperatorCoordinatorEndInputTest.testFailedRecoveryDoesNotPromoteOrAdvanceCheckpoint` | PASS |
| Fatal commit/checkpoint race | coordinator `runCheckpointInEventLoop`, `fail` | `CommittingWriteOperatorCoordinatorTest.testFailedCommitFencesAlreadyQueuedCheckpoint` | PASS |
| ACK before wait | writer `handleOperatorEvent`, `notifyCheckpointComplete` | `CoordinatorCommittingRowDataStoreWriteOperatorTest.testAckBeforeWaitAndDuplicateAck` | PASS |
| ACK during wait | writer mailbox `yield`, current-attempt validation | `CoordinatorCommittingRowDataStoreWriteOperatorTest.testAckDuringTaskMailboxWaitRejectsStaleSignalsAndReentry` | PASS |
| ACK replay after recovery | coordinator `executionAttemptReady`, `sendCommitSuccess`; writer restored marker | `CoordinatorCommittingRowDataStoreWriteOperatorTest.testAckReplayCoversLaterRestoredEmptyMarker`, `CommittingWriteOperatorCoordinatorEndInputTest.testDurableEarlyAckReplayAndAllTerminalRecoveryRelease` | PASS |
| Attempt replacement | coordinator `ackTargets`, `executionAttemptFailed` | `CommittingWriteOperatorCoordinatorEndInputTest.testInFlightCommitUsesOnlyReplacementAckTarget` | PASS |
| All-terminal finalization | coordinator `allTerminal`, `retireAndPromote` | `CommittingWriteOperatorCoordinatorEndInputTest.testFinalizationIsDataFreeAndReleasesAllAfterSuccess`; `CoordinatorCommitITCase.testCoordinatorCommitEndInputFinalizesAndFinishes` | PASS |
| Finalization failure | coordinator `globalFinalizationCompleted`, `fail` | `CommittingWriteOperatorCoordinatorEndInputTest.testFinalizationFailureFencesAckAndCheckpoint` | PASS |
| MAX already published + recovery | `StoreCommitter.filterAndCommit`, coordinator all-terminal `recover` | `CommittingWriteOperatorCoordinatorEndInputTest.testAllTerminalRecoveryPreservesWatermarkAndFiltersMax`, `testPublishedMaxRetriesListenerAcrossRepeatedRecovery` | PASS |
| Listener retry | `CommitListeners.notifyCommittable`; coordinator fatal fence | `CommittingWriteOperatorCoordinatorEndInputTest.testPublishedMaxRetriesListenerAcrossRepeatedRecovery`, `testPublishedMaxRetriesListenerFromAllTerminalState` | PASS |
| `end-input-to-done=true` | `PartitionMarkDoneTrigger.donePartitions`, listener `markDoneByWatermark` | `CoordinatorPartitionFinalizationTest.testSavedNamesSupportMarkAllRecovery` | PASS |
| Unsafe watermark partition recovery | `PartitionMarkDoneListener.create` guard before actions | `CoordinatorPartitionFinalizationTest.testPendingWatermarkRecoveryFailsFastWithoutMarkAll` | Unsupported by design; fail-fast PASS |
| Terminal-state rescale | coordinator `restoreState` parallelism guard | `CommittingWriteOperatorCoordinatorEndInputTest.testTerminalStateRescaleFailsRecovery` | Unsupported by design; fail-fast PASS |
| Region rollback before coverage | coordinator `subtaskReset` coverage guard | `CommittingWriteOperatorCoordinatorEndInputTest.testRegionResetBeforeTerminalCoverageFails` | Unsupported by design; fail-fast PASS |

Additional cross-cutting checks: `CommittingWriteOperatorCoordinatorTest.testPartialFailoverWithoutRestoring` and `testPartialFailoverWithRestoring` cover region failover; `AutoTagForSavepointCommitterOperatorTest` covers savepoint tagging; `WatermarkAlignerTest` plus MiniCluster idle-watermark parity cover watermark handling; `CoordinatorPartitionFinalizationTest.testFilteredOrdinaryReconstructsWatermarkPartitions` proves eligible versus ineligible empty-MAX completion. Serializer compatibility is detailed in `final-review-report.md`.
