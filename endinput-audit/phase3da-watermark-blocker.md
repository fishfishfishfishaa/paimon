# Phase 3D-A: watermark recovery prerequisite

Status: implementation paused at the explicit stop condition in section 7 of the
request: if stored information cannot derive the correct final watermark, stop and
document the missing state. Production/test sources were not modified in this pass.
Earlier phases' code and evidence remain intact.

NO RUNTIME TESTS WERE EXECUTED IN THIS PHASE

## Established watermark semantics

- `FlinkConnectorOptions.END_INPUT_WATERMARK` (line 422) is the optional
  `end-input.watermark` setting, with no default.
- `CommitterOperator.processWatermark` ignores Flink's MAX watermark. Its `endInput`
  replaces `currentWatermark` only when an explicit EndInput watermark is configured.
  Otherwise it retains the ordinary observed watermark. MAX is therefore not an
  established unconditional table watermark.
- The coordinator writer likewise ignores Flink's MAX watermark. Its checkpoint
  contributions retain finite watermark/idle information.
- `WatermarkAligner` starts at MIN and is deliberately not checkpointed. The existing
  coordinator applies its ordinary alignment before commit and terminal promotion.

## Exact missing information

There is no durable value for the last aligned ordinary watermark when all-terminal
recovery proceeds with zero writer reporters and no explicit EndInput watermark.

`CoordinatorState` stores commit user, committer auxiliary state and terminal coverage,
but the current implementation does not put the aligned watermark into auxiliary
state. Successfully processed writer entries are retired. Durable terminal writers
need not replay their operator state. A fresh aligner cannot reconstruct the old
watermark from checkpoint identifiers alone.

Concrete indistinguishable histories supported by the current Phase 3C code:

```text
Existing table snapshot watermark = 50
No end-input.watermark override; empty snapshots are not forced

History A: final empty writer contributions align to 100
History B: final empty writer contributions align to 200

Both ordinary empty commits return successfully
Both establish identical terminalCoveredBy checkpoint IDs
Both later coordinator checkpoints persist those terminal facts
Neither empty commit necessarily advances the table snapshot

Recover the later coordinator checkpoint with zero writer reporters:
  same terminal coverage
  same existing snapshot watermark 50
  fresh aligner watermark MIN
  no retained information distinguishing 100 from 200
```

The live default final watermark differs between A and B, but the permitted restored
inputs do not distinguish them. A configured explicit watermark avoids this ambiguity;
it does not solve the supported unconfigured case.

Reading the latest table snapshot alone is insufficient. FileStoreCommitImpl retains
the maximum of the supplied watermark and the latest snapshot watermark when creating
a snapshot, but that does not recover a higher watermark from an empty contribution
that never produced a snapshot. The coordinator only constructs a forced ordinary
empty manifest when `committer.forceCreatingSnapshot()` is true.

## Infrastructure already suitable for the next implementation

`StoreCommitter.combine(MAX, resolvedWatermark, Collections.emptyList())` can construct
a data-free token. `filterAndCommit` provides the existing publication filtering and
listener notification path. No public finalizeEndInput API is needed to express it.
Its empty-snapshot behavior must still be respected; token delivery alone must not
be reported as proof that a MAX snapshot was created under every configuration.

Finalization can run synchronously after ordinary successful retirement/promotion,
and after zero-reporter reconciliation. A runtime-only completion flag can then gate
the shared ACK send helper and release all currently ready terminal targets. These
changes have not been made while the watermark prerequisite remains unresolved.

## Listener boundary confirmed by inspection

Process-time partition mark-done detects an empty MAX and can use existing pending
partition tracking when enabled. Watermark-mode mark-done derives partition watermarks
from file messages and returns early when those are absent—even if MAX is present.
It therefore does not currently complete pending partitions from an empty MAX token.
This must not be claimed as supported without the permitted small hook or Phase 3D-B
correction. Existing filtering does not imply exactly-once listener side effects.

## Required scope decision

A minimal candidate is to persist the last successfully processed aligned watermark
alongside terminal coverage using the existing committer auxiliary-state map, without
changing CoordinatorState's outer layout and without adding any finalization-success
or ACK flags. It would be restored before zero-reporter reconciliation. Compatibility
for old all-terminal state without that value must be explicit; the missing watermark
cannot be retroactively recovered in the example above.

This is a proposal, not an implemented or approved state change. The request explicitly
requires stopping before introducing an arbitrary watermark or unavoidable new durable
information. Confirmation of this extension (including old-state behavior), or an
explicit restriction to configured end-input.watermark, is needed to proceed.

## Validation

Read-only source and diff inspection only. `git diff --check` passed; Git separately
reported an existing LF/CRLF normalization warning for
`CommittingWriteOperatorCoordinatorTest.java`. No compilation was necessary because
no Java sources were changed. No runtime commands were executed.

Phase 3D-A is not complete. No new final ACK, MAX publication or watermark behavior
is claimed by this report. Phase 3D-B was not started.
