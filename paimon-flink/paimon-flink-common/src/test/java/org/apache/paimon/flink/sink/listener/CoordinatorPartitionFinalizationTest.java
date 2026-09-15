/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.state.MemoryBackendStateStore;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Support boundaries for coordinator partition finalization without new durable state. */
public class CoordinatorPartitionFinalizationTest extends TableTestBase {

    @Override
    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(tempPath.toUri());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    @Test
    public void testFilteredOrdinaryReconstructsWatermarkPartitions() throws Exception {
        checkFilteredReplay("watermark");
    }

    @Test
    public void testFilteredOrdinaryReconstructsProcessTimePartitions() throws Exception {
        checkFilteredReplay("process-time");
    }

    private void checkFilteredReplay(String mode) throws Exception {
        FileStoreTable table = table(mode, !mode.equals("watermark"));
        MemoryBackendStateStore state = new MemoryBackendStateStore();
        StoreCommitter original = committer(table, state, false);
        ManifestCommittable ordinary = ordinary(table);
        original.commit(Collections.singletonList(ordinary));
        Path done = new Path(table.location(), "a=2025-03-01 12/_SUCCESS");
        assertThat(table.fileIO().exists(done)).isFalse();
        original.close();
        // No saved tracking: only the original, already committed replay can supply p3.
        StoreCommitter restored = committer(table, new MemoryBackendStateStore(), true);
        assertThat(restored.filterAndCommit(Collections.singletonList(ordinary), true, true))
                .isZero();
        assertThat(table.fileIO().exists(done)).isFalse();
        ManifestCommittable end =
                new ManifestCommittable(
                        Long.MAX_VALUE, Instant.parse("2025-03-01T14:00:00Z").toEpochMilli());
        restored.filterAndCommit(Collections.singletonList(end), false, true);
        assertThat(table.fileIO().exists(done)).isTrue();
        if (mode.equals("watermark")) {
            assertThat(
                            table.fileIO()
                                    .exists(new Path(table.location(), "a=2025-03-01 14/_SUCCESS")))
                    .isFalse();
        }
        long snapshot = table.snapshotManager().latestSnapshotId();
        restored.filterAndCommit(Collections.singletonList(end), false, true);
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
        restored.close();
    }

    @Test
    public void testPendingWatermarkRecoveryFailsFastWithoutMarkAll() throws Exception {
        FileStoreTable table = table("watermark", false);
        MemoryBackendStateStore state = new MemoryBackendStateStore();
        StoreCommitter original = committer(table, state, false);
        original.commit(Collections.singletonList(ordinary(table)));
        original.snapshotState();
        MemoryBackendStateStore restored = new MemoryBackendStateStore(state.getSerializedStates());
        original.close();
        assertThatThrownBy(() -> committer(table, restored, true))
                .hasStackTraceContaining("per-partition watermarks are not persisted");
        assertThat(table.fileIO().exists(new Path(table.location(), "a=2025-03-01 12/_SUCCESS")))
                .isFalse();
    }

    @Test
    public void testSavedNamesSupportMarkAllRecovery() throws Exception {
        FileStoreTable table = table("watermark", true);
        MemoryBackendStateStore state = new MemoryBackendStateStore();
        StoreCommitter original = committer(table, state, false);
        original.commit(Collections.singletonList(ordinary(table)));
        original.snapshotState();
        MemoryBackendStateStore saved = new MemoryBackendStateStore(state.getSerializedStates());
        original.close();
        StoreCommitter restored = committer(table, saved, true);
        restored.filterAndCommit(
                Collections.singletonList(new ManifestCommittable(Long.MAX_VALUE, 0L)),
                false,
                true);
        assertThat(table.fileIO().exists(new Path(table.location(), "a=2025-03-01 12/_SUCCESS")))
                .isTrue();
        restored.close();
    }

    private StoreCommitter committer(
            FileStoreTable table, MemoryBackendStateStore state, boolean restored) {
        return new StoreCommitter(
                table,
                table.newCommit("user").ignoreEmptyCommit(false),
                Committer.createContext("user", null, true, restored, state, 1, 0));
    }

    private ManifestCommittable ordinary(FileStoreTable table) throws Exception {
        try (TableWriteImpl<?> write = table.newWrite("user")) {
            write.write(GenericRow.of(BinaryString.fromString("2025-03-01 12"), 1));
            write.write(GenericRow.of(BinaryString.fromString("2025-03-01 14"), 2));
            ManifestCommittable result =
                    new ManifestCommittable(
                            1, Instant.parse("2025-03-01T12:50:00Z").toEpochMilli());
            write.prepareCommit(true, 1).forEach(result::addFileCommittable);
            return result;
        }
    }

    private FileStoreTable table(String mode, boolean markAll) throws Exception {
        Identifier name = identifier("finalization");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.STRING())
                        .column("b", DataTypes.INT())
                        .partitionKeys("a")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.FILE_FORMAT.key(), "avro")
                        .option(CoreOptions.PARTITION_MARK_DONE_ACTION.key(), "success-file")
                        .option(
                                CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT.key(),
                                Boolean.toString(markAll))
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd HH")
                        .option(FlinkConnectorOptions.PARTITION_MARK_DONE_MODE.key(), mode)
                        .option(FlinkConnectorOptions.PARTITION_TIME_INTERVAL.key(), "1 h")
                        .option(
                                FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE.key(),
                                mode.equals("process-time") ? "10000 d" : "15 min")
                        .option(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED.key(), "true")
                        .build();
        catalog.createTable(name, schema, true);
        return (FileStoreTable) catalog.getTable(name);
    }
}
