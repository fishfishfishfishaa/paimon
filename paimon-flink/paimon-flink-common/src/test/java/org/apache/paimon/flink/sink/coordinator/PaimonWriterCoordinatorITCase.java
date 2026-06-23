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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for {@link PaimonWriterCoordinator}. */
@SuppressWarnings("BusyWait")
public class PaimonWriterCoordinatorITCase extends CatalogITCaseBase {

    private static final String MINI_CLUSTER_FIELD = "miniCluster";
    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("v", new VarCharType())));

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE unaware_table (k INT, v STRING) WITH ("
                        + "'bucket'='-1',"
                        + "'sink.committer-coordinator-operator.enabled'='true')",
                "CREATE TABLE fixed_table (k INT, v STRING) WITH ("
                        + "'bucket'='1',"
                        + "'bucket-key'='k',"
                        + "'sink.committer-coordinator-operator.enabled'='true')",
                "CREATE TABLE dynamic_table (k INT PRIMARY KEY NOT ENFORCED, v STRING) WITH ("
                        + "'bucket'='-1',"
                        + "'sink.committer-coordinator-operator.enabled'='true')");
    }

    @Test
    @Timeout(120)
    public void testStreamingCheckpointWriteUnawareTableWithWriterCoordinator() throws Exception {
        testStreamingCheckpointWriteWithWriterCoordinator("unaware_table");
    }

    @Test
    @Timeout(120)
    public void testFixedTableRejectsWriterCoordinator() throws Exception {
        assertThatThrownBy(() -> buildPaimonSink("fixed_table"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("committer operator is the main region failover boundary");
    }

    @Test
    @Timeout(120)
    public void testDynamicTableRejectsWriterCoordinator() throws Exception {
        assertThatThrownBy(() -> buildPaimonSink("dynamic_table"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("committer operator is the main region failover boundary");
    }

    private void testStreamingCheckpointWriteWithWriterCoordinator(String tableName)
            throws Exception {
        StreamExecutionEnvironment env = buildPaimonSink(tableName);

        JobClient jobClient = env.executeAsync();
        triggerCheckpointAndWaitForWrites(jobClient, tableName, 4);
        jobClient.cancel().get();

        sqlAssertWithRetry(
                "SELECT * FROM " + tableName,
                rows ->
                        rows.containsExactlyInAnyOrder(
                                Row.of(1, "one"),
                                Row.of(2, "two"),
                                Row.of(3, "three"),
                                Row.of(4, "four")));
    }

    private StreamExecutionEnvironment buildPaimonSink(String tableName) throws Exception {
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(100)
                        .build();

        new FlinkSinkBuilder(paimonTable(tableName))
                .forRowData(
                        env.addSource(new EmitOnceAndWaitSource())
                                .returns(InternalTypeInfo.of(ROW_TYPE))
                                .setParallelism(1))
                .build();
        return env;
    }

    @SuppressWarnings("unchecked")
    private <T> T reflectGetMiniCluster(Object instance)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(MINI_CLUSTER_FIELD);
        field.setAccessible(true);
        return (T) field.get(instance);
    }

    private void triggerCheckpointAndWaitForWrites(
            JobClient jobClient, String tableName, long totalRecords) throws Exception {
        MiniCluster miniCluster = reflectGetMiniCluster(jobClient);
        JobID jobID = jobClient.getJobID();
        waitForJobRunning(jobClient, miniCluster, jobID);

        long lastSnapshotId = -1L;
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            miniCluster.triggerCheckpoint(jobID).get();
            Snapshot snapshot = waitForNewSnapshot(tableName, lastSnapshotId, deadline);
            lastSnapshotId = snapshot.id();
            if (snapshot.totalRecordCount() >= totalRecords) {
                return;
            }
        }
        throw new AssertionError("Timed out waiting for records committed by PWC.");
    }

    private void waitForJobRunning(JobClient jobClient, MiniCluster miniCluster, JobID jobID)
            throws Exception {
        JobStatus jobStatus = jobClient.getJobStatus().get();
        while (jobStatus == JobStatus.INITIALIZING || jobStatus == JobStatus.CREATED) {
            Thread.sleep(500L);
            jobStatus = jobClient.getJobStatus().get();
        }

        if (jobStatus != JobStatus.RUNNING) {
            throw new IllegalStateException("Job status is not RUNNING");
        }

        AtomicBoolean allTaskRunning = new AtomicBoolean(false);
        while (!allTaskRunning.get()) {
            allTaskRunning.set(true);
            Thread.sleep(500L);
            miniCluster
                    .getExecutionGraph(jobID)
                    .thenAccept(
                            graph ->
                                    graph.getAllExecutionVertices()
                                            .forEach(
                                                    vertex -> {
                                                        if (vertex.getExecutionState()
                                                                != ExecutionState.RUNNING) {
                                                            allTaskRunning.set(false);
                                                        }
                                                    }))
                    .get();
        }
    }

    private Snapshot waitForNewSnapshot(String tableName, long initialSnapshotId, long deadline)
            throws InterruptedException {
        Snapshot snapshot = findLatestSnapshot(tableName);
        while (System.currentTimeMillis() < deadline
                && (snapshot == null || snapshot.id() == initialSnapshotId)) {
            Thread.sleep(500L);
            snapshot = findLatestSnapshot(tableName);
        }
        if (snapshot == null || snapshot.id() == initialSnapshotId) {
            throw new AssertionError("Timed out waiting for a new Paimon snapshot.");
        }
        return snapshot;
    }

    private static class EmitOnceAndWaitSource extends RichParallelSourceFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(row(1, "one"));
                ctx.collect(row(2, "two"));
                ctx.collect(row(3, "three"));
                ctx.collect(row(4, "four"));
            }
            while (running) {
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private static RowData row(int k, String v) {
            return GenericRowData.of(k, StringData.fromString(v));
        }
    }
}
