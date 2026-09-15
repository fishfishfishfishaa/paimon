# Phase 3D-B final report

Validated 2026-09-15 on the existing local branch. This report supersedes the blocked inspection in `phase3db-recovery-gap.md` under the user's subsequently clarified scope: retain global watermark, add no per-partition durable watermark, fail fast for unsupported restoration, and leave HTTP/custom action idempotency contracts outside this PR.

## A. Production changes

`StoreCommitter` now separates coordinator recovery publication filtering from listener delivery. It reconstructs partition tracking before filtering, processes entries in identifier order through the existing table commit API, and notifies listeners only for newly committed ordinary entries and every MAX finalization token. Other commit paths retain their existing filtering behavior.

`CommitListeners` exposes narrowly scoped pure partition tracking reconstruction. `PartitionMarkDoneListener` reconstructs process-time or watermark tracking and consumes the watermark of data-free MAX. `PartitionMarkDoneTrigger` exposes whether restored pending names exist, used by the fail-fast guard. No new durable state or serializer version was introduced in D-B; the existing global watermark from D-A remains.

## B. Listener tracking reconstruction path

Original recovered ordinary committables -> partition extraction and in-memory tracking -> publication filtering -> listener delivery for newly published entries only. A filtered ordinary entry therefore restores its partitions without repeating ordinary external listener effects. Tests publish ordinary data, restore with no saved tracking, replay the filtered entry, and verify completion at MAX for both process-time and watermark modes.

## C. MAX publication versus listener retry

Filtering an already published MAX does not suppress listener delivery. Coordinator finalization succeeds only when `filterAndCommit` returns after all listeners return. A listener exception enters the existing fatal fence; the in-memory completion flag cannot hide an exception. Publication is not duplicated on subsequent attempts. HTTP/custom action retry guarantees are not introduced or enforced by this PR.

## D. Watermark-mode empty MAX and support boundary

The MAX token still has no files. Its explicit watermark evaluates existing partition tracking. The final watermark boundary test uses two partitions: the eligible one receives `_SUCCESS`; the later ineligible partition does not when `partition.end-input-to-done=false`.

Supported: fresh tracking; tracking reconstructed from original ordinary replay when the restored pending-name state is empty; and `partition.end-input-to-done=true`, which can finish saved pending names without recovering their former per-partition watermarks. Saved-name restoration with mark-all is tested with a final watermark of zero to prove it does not depend on reconstructed event-time values.

Unsupported: coordinator watermark-mode restoration with nonempty saved pending names and `partition.end-input-to-done=false`. Listener construction throws a descriptive exception before actions are created. This deliberately conservative guard rejects the combination even if a particular replay might reconstruct every name; no coverage-tracking protocol is added. The exception explains the missing per-partition watermarks and the available configuration alternatives. Global watermark alone is not substituted for per-partition watermark.

## E. MAX-success/listener-failure recovery timeline

The integration-level coordinator unit tests use real StoreCommitter and snapshot publication, with a service-loaded listener that throws on its first two MAX notifications. Attempt 1 publishes ordinary data and MAX, throws, emits no ACK, and rejects a subsequent coordinator checkpoint through the fatal fence. Attempts 2 and 3 restore and reuse the same MAX publication. Both writer-payload replay and direct all-terminal restoration with zero required reporters are covered.

## F. Repeated recovery behavior

Each three-attempt test asserts: identical latest snapshot ID throughout; latest identifier MAX; one copy of the ordinary row; three MAX listener calls; one ordinary listener call. Attempt 2 fails again without ACK; attempt 3 returns successfully and releases the current attempt. No large MiniCluster failure-injection framework was added; repeated failures use real publication under the coordinator test harness.

## G. ACK timing

Both failing attempts have no ACK, and final ACK requires listener success. Existing writer/coordinator tests continue to cover early-terminal retirement, failure fencing and current-attempt delivery. Recovery tests now accept protocol-permitted duplicate ACKs while checking recipient, attempt and checkpoint coverage rather than an unstable exact event count. The asymmetric MiniCluster case also explicitly asserts no MAX while the other writer remains active.

## H. MiniCluster results

Both required cases passed in the final normal-profile run (13.125 seconds combined):

- `testCoordinatorCommitEndInputFinalizesAndFinishes`: job FINISHED, latest identifier MAX, exactly two ordinary rows.
- `testEarlyTerminalWriterWaitsForCoordinatorCommitAck`: while listener is blocked neither writer finishes; after release one writer FINISHED, the other/job RUNNING, one row, no MAX.

An initial bounded run timed out waiting for job completion after data publication. A diagnostic rerun with Flink INFO logs passed both cases, and the subsequent full normal-profile run also passed both without the logging override. The initial timing failure has not been conclusively attributed; preserve it as a broad-regression follow-up, not as a claim of a diagnosed/fixed cause. Evidence: `phase3db-minicluster.log`, `phase3db-minicluster-debug.log`, `phase3db-final-validation.log`.

## I. Focused regression counts and static validation

Final normal-profile run: **121 tests, 0 failures, 0 errors, 0 skipped** (119 unit tests + 2 MiniCluster cases). No fast-build profile. Checkstyle, scoped Spotless, Maven enforcer rules and compilation passed. Spotless scope covers D-B touched Java files; repository-wide formatting and the verify-phase RAT goal are not claimed.

| Test class | Count |
| --- | ---: |
| CoordinatorCommittingRowDataStoreWriteOperatorTest | 18 |
| CommittingWriteOperatorCoordinatorTest | 26 |
| CommittingWriteOperatorCoordinatorEndInputTest | 27 |
| WriterCommittablesTest | 14 |
| CheckpointCommittablesSerializerTest | 6 |
| CoordinatorStateSerializerTest | 6 |
| CommittableEventTest | 2 |
| RestoredCommittableEventTest | 2 |
| WatermarkAlignerTest | 6 |
| PartitionMarkDoneTest | 4 |
| WatermarkPartitionMarkDoneTest | 1 |
| PartitionMarkDoneTriggerTest | 3 |
| CoordinatorPartitionFinalizationTest | 4 |
| CoordinatorCommitITCase (selected methods) | 2 |

After the full run, the negative watermark eligibility assertion was strengthened; its class was rerun with normal checks: **4 passed**, evidence `phase3db-watermark-boundary.log`. These are reruns, not four additional unique tests. `git diff --check` passed (only the existing CRLF conversion warning for CommittingWriteOperatorCoordinatorTest).

The two existing partition listener suites needed Windows-safe URI construction in their test setup. Their traceable filesystem remains enabled. New finalization tests use the local catalog URI. Early iterations also exposed and corrected a service-registration path, duplicate-ACK assertions, and test-helper Checkstyle requirements.

## J. Remaining real limitations

- Nonempty pending-name restoration in coordinator watermark mode with mark-all disabled is explicitly unsupported as described in D; changing to mark-all changes the completion policy and must be intentional.
- HTTP/custom action idempotency and externally exactly-once effects remain outside this PR. Retrying MAX is not a promise that arbitrary external actions are idempotent.
- No new per-partition durable watermark or recovery coverage protocol was added.
- The first bounded MiniCluster timeout remains recorded for broader regression investigation despite two subsequent passes.
- Coordinator recovery calls the existing filter/commit API per entry to distinguish filtered ordinary publications without adding a core API. Large replay-batch performance is not benchmarked here.
- No broad cross-version matrix or real MiniCluster restart-injection scenario was run. Those are not prerequisites added to this final implementation phase.

Next work is cleanup, broad regression and PR preparation only.

ENDINPUT PROTOCOL IMPLEMENTATION COMPLETE
