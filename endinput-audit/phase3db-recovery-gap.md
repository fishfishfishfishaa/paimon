# Phase 3D-B inspection: missing partition watermark durability

This pass stopped under section 7 of the request: do not automatically introduce new
durable state when a recovery gap is found. No production or test sources were modified.
No runtime tests were executed. Existing Phase 1–3D-A work remains intact.

## A. Production changes

None in this pass. Inspected StoreCommitter, CommitListeners, PartitionMarkDoneListener,
PartitionMarkDoneTrigger and the built-in partition actions. The gap below precedes a
correct implementation of watermark-mode empty-MAX recovery.

## B. Listener tracking reconstruction path

The live PartitionMarkDoneTrigger keeps `Map<String, Long> pendingPartitions`. In
watermark mode the Long is the partition's last reported watermark. Eligibility
depends on this value, the partition interval end and the final/global watermark.

However, its durable descriptor is `ListState<List<String>>`, named
`mark-done-pending-partitions`. `snapshotState()` (line 218) saves only the keys.
The constructor (line 103) restores each key with `currentTimeMillis`, including
when the caller uses watermark mode. Per-partition event-time values are lost.

Pure reconstruction from original recovered ordinary committables can repair writers
that actually replay those committables. It cannot repair an already durably terminal
writer excluded from recovery participation. Its successful pending contributions
were retired, and its original terminal payload must not be required again.

## C. MAX publication versus listener retry

StoreCommitter.filterAndCommit already passes the original list to CommitListeners
after underlying snapshot filtering. Thus a filtered MAX can still invoke listeners;
filtering alone does not set coordinator finalization success. The current coordinator
sets its runtime completion flag only after the synchronous call returns.

Ordinary recovery currently also invokes ordinary listener notifications. A pure
tracking-only reconstruction hook is still needed to avoid blindly replaying ordinary
external effects. Adding that hook alone cannot recover absent per-partition watermarks.

## D. Watermark-mode empty-MAX gap

PartitionMarkDoneListener currently returns early when no file-derived partition
watermark is found. Reading the final watermark directly from empty MAX would fix the
live empty-token trigger, but it would not fix its recovered input state.

For `partition.end-input-to-done=false` (the default), donePartitions uses the actual watermark
eligibility comparison (trigger lines 176 and 184):

```text
finalWatermark - max(partitionLastWatermark, partitionIntervalEnd) > idleTime
```

Using restored wall-clock time in place of partitionLastWatermark is not equivalent.
Using MIN, the partition interval end alone, or the single global saved watermark
would also change which partitions are eligible.

When mark-done-on-EndInput is true, the existing trigger bypasses this comparison and
returns all pending names. That configuration does not establish correctness for the
supported watermark-eligibility configuration above.

## E. Exact failure/recovery timeline

Consider watermark-mode partition p3, interval end 13:00 UTC, idle threshold 30 minutes,
and mark-done-on-EndInput=false. Ordinary external actions have not yet marked p3 done.

```text
History A: p3 last reported watermark = 12:10
History B: p3 last reported watermark = 13:20

W1 advances the aligned watermark to 13:25 in both histories.
p3 is still pending in both:
  A: 13:25 - max(12:10, 13:00) = 25 minutes
  B: 13:25 - max(13:20, 13:00) = 5 minutes

W0, which supplied p3, becomes durably terminal.
Checkpoint R saves W0 coverage, pending name p3, global watermark 13:25.
W1 is still active.

W1 later commits its terminal tail; final global watermark is 13:40.
MAX snapshot is published.
A listener fails before all partition-finalization actions complete.
Fatal fence prevents ACK and a newer successful coordinator checkpoint.

Recover R:
  W0 is durably terminal, so no W0 restore event is required.
  W1 replays its own tail, recovering final global watermark 13:40.
  MAX can be filtered as already published.
  p3's pending name is restored, but its last watermark is not.
```

Correct p3 eligibility differs:

```text
A: 13:40 - 13:00 = 40 minutes -> eligible
B: 13:40 - 13:20 = 20 minutes -> not eligible
```

Available recovery state has identical W0 terminal coverage, pending partition names
and global watermark in these histories. W1's replay does not contain p3. The latest
MAX snapshot supplies the same final watermark, not p3's last watermark. The old
ordinary snapshot history is not a guaranteed retained reconstruction log (snapshot
expiration is supported); reading arbitrary historic snapshots would itself require
a new retention/reconstruction contract.

Missing information: p3's last event-time watermark. No change to MAX filtering or
pure reconstruction from W1's events can infer it.

## F. Repeated recovery and built-in action boundary

Repeated recovery reproduces the same loss. An in-memory finalization-success flag
cannot fix it and must not conceal it.

There is also an external retry-contract limitation to resolve explicitly:
`HttpReportMarkDoneAction.markDone` posts table/path/partition/params with empty headers
and then checks the response. The receiver can apply a non-idempotent effect and lose
the response; recovery sends the request again. This implementation supplies no
enforced receiver deduplication or transactional acknowledgement contract. For example,
a receiver appending a notification on every accepted POST can produce duplicates.
Retry safety is conditional on the endpoint's behavior, not guaranteed by the built-in
action. This does not assert that every HTTP endpoint is unsafe.

Success-file action rewrites the same file but updates modification time; retries are
not strictly identical external effects. A final guarantee must state which effects
and action configurations are permitted rather than claim universal exactly-once.

## G. ACK timing

Existing code still withholds final ACK until synchronous finalization returns. It
must remain unchanged until the recovered tracking is correct; simply allowing an
empty-MAX listener call to return cannot establish that all required effects occurred.
No new ACK behavior was implemented in this pass.

## H. MiniCluster results

Not run in this pass. The request explicitly says to stop and report a new durability
gap rather than introduce new state automatically. No current-phase FINISHED or
failover success is claimed for either bounded or asymmetric IT.

## I. Focused regression counts

Executed in this pass: 0 runtime tests. Read-only code inspection and
`git diff --check` were performed; the latter passed with an existing LF/CRLF warning
for CommittingWriteOperatorCoordinatorTest.java. Prior reports' counts are not reused
as Phase 3D-B validation.

## J. Minimal possible fixes and real limitations

1. Preserve per-partition watermark values in listener auxiliary state for watermark
   mode, with explicit old-state compatibility. Keep process-time restoration behavior
   separate. This needs approval under the request's no-new-durable-state stop rule;
   the previously approved single global watermark does not provide these values.
2. Add the narrow pure tracking-reconstruction hook for original ordinary replay before
   publication filtering, without invoking ordinary external actions for filtered data.
   This complements durable partition values; it cannot replace them for absent writers.
3. For old watermark state containing only names, require complete original replay or
   fail explicitly when eligibility cannot be reconstructed. A semantic restriction to
   mark-all-on-EndInput is an alternative only if deliberately approved; it must not be
   silently substituted for watermark-based eligibility.
4. Define retry safety for HTTP/custom actions: require an idempotent endpoint/action
   contract or explicitly reject unsupported configurations. Client-side flags cannot
   resolve an external effect that succeeded before its response was lost.

No additional phase was created, and none of these state/contract changes was applied
automatically. Phase 3D-B is incomplete pending a decision on these prerequisites.

BLOCKED BY NEW DURABILITY GAP
