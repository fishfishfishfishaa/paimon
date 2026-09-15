# Coordinator-commit EndInput protocol

## 1. Problem

The former coordinator path let each writer emit a data-bearing `Long.MAX_VALUE` committable when its input ended. One early writer could therefore advance publication/finalization ahead of active writers, while later ordinary checkpoints and regional recovery still needed that writer's contribution. A MAX identifier also cannot identify the real checkpoint that made its tail durable.

## 2. Final writer protocol

`endInput()` seals input. The writer prepares its tail at the next real checkpoint barrier K, persists a `CheckpointCommittables(K, ..., terminal=true)` entry, and reports it from `snapshotState`. Before the terminal barrier it keeps ordinary downstream metrics behavior. After terminal capture it emits empty terminal markers at later real checkpoints and waits for a coordinator success event covering its terminal boundary before completing the checkpoint notification.

## 3. Terminal durability

The coordinator commits ordinary K first, then promotes the terminal candidate to `terminalCoveredBy[writer] = K`. That per-writer coverage is checkpointed with the commit user and auxiliary state. A covered writer no longer contributes to later watermark minima or blocks alignment. A region reset before saved coverage fails fast rather than treating uncommitted tail files as retired.

## 4. Recovery

Restoring the first terminal boundary K still requires that writer's original pending contribution, because the coordinator checkpoint precedes writer snapshots. At a later durable boundary L, terminal coverage is already saved and that writer need not report again. If every writer is covered, recovery can reconcile finalization with zero writer reporters. Uncovered writers must replay before ordinary recovery commit. Saved terminal coverage requires the original writer parallelism; legacy state without coverage follows ordinary restore and needs a configured final watermark if it otherwise cannot resolve one.

## 5. Completion

After an ordinary checkpoint commit succeeds, the coordinator retires/promotes terminal candidates. It sends an early ACK only to newly covered writers while others remain active. Once every writer is terminal, it publishes global finalization and sends a final ACK to all current eligible attempts only after every listener returns. Attempts are validated at the event gateway and on receipt; replay after reset covers later empty terminal markers. A fatal commit failure fences queued checkpoints and ACKs.

## 6. Global finalization

One data-free `Long.MAX_VALUE` manifest committable carries the resolved EndInput/global watermark. It creates the final snapshot without fabricated files. Partition tracking from original ordinary replay is rebuilt before publication filtering. An already published MAX is filtered as a snapshot but still delivered to listeners for required effects. The watermark-mode partition listener uses the empty MAX watermark to evaluate eligible pending partitions; process-time behavior is retained. `partition.end-input-to-done=true` can finish restored pending names without their old event-time values.

## 7. Failure semantics

MAX publication and global finalization completion are distinct. If publication succeeds and a listener throws, the coordinator fails the job and sends no final ACK. A fresh coordinator filters the published MAX, retries the listener, and ACKs only after success. Repeated failures retain one MAX snapshot and one copy of ordinary data. Tracking reconstruction itself invokes no external partition action.

## 8. Explicit boundaries

Terminal-state rescale is rejected. Coordinator watermark-mode restoration with nonempty saved pending partition names and `partition.end-input-to-done=false` is rejected before actions, since name-only state cannot recover per-partition watermarks. A configured global watermark is not a substitute. HTTP/custom listener idempotency is an external action contract outside this change. The existing non-coordinator `CommitterOperator` MAX path remains intact.

## 9. Testing

Focused writer/coordinator, serializer, partition listener and real-committer retry tests cover terminal capture, abort inheritance, first/later restoration, zero reporters, ACK timing, fatal fencing, empty MAX, listener retry and compatibility. Broader ordinary commit, writer, savepoint/tagging and coordinator tests run under `-Pflink1`. MiniCluster tests check bounded FINISHED/MAX/exact row count and asymmetric early-writer FINISHED while the other writer/job remain RUNNING with no MAX. See `final-review-report.md` for exact commands and per-run results.
