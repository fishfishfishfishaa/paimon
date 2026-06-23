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

import org.apache.paimon.flink.sink.Committable;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Tracks writer subtasks and pending checkpoint file information for PWC. */
public class PendingSubtask {

    private final Map<Integer, Map<Integer, OperatorCoordinator.SubtaskGateway>> registeredSubtasks;
    private final Map<Long, PendingCheckpoint> checkpoints;
    private final Set<Long> abortedCheckpoints;
    private final Set<Integer> recoveryCompleteSubtasks;
    private final CommitterCoordinator<Committable, ?> coordinator;

    private int parallelism;
    private long maxCommittedCheckpointId;
    private long restoredCheckpointId;

    public PendingSubtask(CommitterCoordinator<Committable, ?> coordinator) {
        this.coordinator = coordinator;
        this.registeredSubtasks = new HashMap<>();
        this.checkpoints = new HashMap<>();
        this.abortedCheckpoints = new HashSet<>();
        this.recoveryCompleteSubtasks = new HashSet<>();
        this.maxCommittedCheckpointId = Long.MIN_VALUE;
        this.restoredCheckpointId = Long.MIN_VALUE;
    }

    public void init(int parallelism) {
        this.parallelism = parallelism;
    }

    public void registerSubtask(
            int subtask, int attemptNumber, OperatorCoordinator.SubtaskGateway gateway) {
        registeredSubtasks
                .computeIfAbsent(subtask, ignored -> new HashMap<>())
                .put(attemptNumber, gateway);
    }

    public void unregisterSubtask(int subtask, int attemptNumber, Throwable throwable) {
        Map<Integer, OperatorCoordinator.SubtaskGateway> attempts = registeredSubtasks.get(subtask);
        if (attempts != null) {
            attempts.remove(attemptNumber);
        }
    }

    public boolean isValid(int subtask, int attemptNumber) {
        Map<Integer, OperatorCoordinator.SubtaskGateway> attempts = registeredSubtasks.get(subtask);
        return attempts != null && attempts.containsKey(attemptNumber);
    }

    public Collection<OperatorCoordinator.SubtaskGateway> activeGateways() {
        Collection<OperatorCoordinator.SubtaskGateway> gateways = new ArrayList<>();
        for (Map<Integer, OperatorCoordinator.SubtaskGateway> attempts :
                registeredSubtasks.values()) {
            gateways.addAll(attempts.values());
        }
        return gateways;
    }

    public CommitResult receive(int subtask, FileInfoRequest request) throws Exception {
        long checkpointId = request.checkpointId();
        if (checkpointId <= maxCommittedCheckpointId) {
            return new CommitResult(true, 0, maxCommittedCheckpointId, false);
        }
        if (abortedCheckpoints.contains(checkpointId)) {
            return CommitResult.NONE;
        }

        PendingCheckpoint checkpoint = checkpoint(checkpointId);
        checkpoint.receive(subtask, request);
        return CommitResult.NONE;
    }

    public CommitResult receiveRecoveryComplete(int subtask) throws Exception {
        if (restoredCheckpointId == Long.MIN_VALUE) {
            return CommitResult.NONE;
        }
        if (restoredCheckpointId <= maxCommittedCheckpointId) {
            return new CommitResult(true, 0, maxCommittedCheckpointId, false);
        }

        recoveryCompleteSubtasks.add(subtask);
        if (!recoveryCompleteSubtasks.containsAll(expectedSubtasks())) {
            return CommitResult.NONE;
        }

        saveLatestCompleteCheckpointUpTo(restoredCheckpointId);
        int committedCount = coordinator.filterAndCommitUpToCheckpoint(restoredCheckpointId);
        maxCommittedCheckpointId = Math.max(maxCommittedCheckpointId, restoredCheckpointId);
        cleanupCommittedCheckpoints(restoredCheckpointId);
        return new CommitResult(true, committedCount, restoredCheckpointId, true);
    }

    public CommitResult notifyCheckpointComplete(long checkpointId) throws Exception {
        if (checkpointId <= maxCommittedCheckpointId) {
            return new CommitResult(true, 0, maxCommittedCheckpointId, false);
        }
        PendingCheckpoint checkpoint = checkpoints.get(checkpointId);
        if (checkpoint == null || !checkpoint.allReceived()) {
            throw new IllegalStateException(
                    String.format(
                            "Checkpoint %d completed before PWC received file info from all subtasks.",
                            checkpointId));
        }

        saveCheckpoint(checkpoint);
        coordinator.notifyCheckpointComplete(checkpointId);
        maxCommittedCheckpointId = Math.max(maxCommittedCheckpointId, checkpointId);
        cleanupCommittedCheckpoints(checkpointId);
        return new CommitResult(true, 0, checkpointId, false);
    }

    public void notifyCheckpointAborted(long checkpointId) {
        abortedCheckpoints.add(checkpointId);
        checkpoints.remove(checkpointId);
        coordinator.notifyCheckpointAborted(checkpointId);
    }

    public void restoreCheckpoint(long checkpointId) {
        restoredCheckpointId = checkpointId;
        recoveryCompleteSubtasks.clear();
    }

    private PendingCheckpoint checkpoint(long checkpointId) {
        return checkpoints.computeIfAbsent(
                checkpointId, ignored -> new PendingCheckpoint(checkpointId, expectedSubtasks()));
    }

    private Set<Integer> expectedSubtasks() {
        Set<Integer> subtasks = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            subtasks.add(i);
        }
        return subtasks;
    }

    private void saveCheckpoint(PendingCheckpoint checkpoint) throws Exception {
        coordinator.save(
                checkpoint.committablesAfter(maxCommittedCheckpointId),
                checkpoint.checkpointId(),
                checkpoint.maxWatermark());
    }

    private void saveLatestCompleteCheckpointUpTo(long checkpointId) throws Exception {
        PendingCheckpoint latest = null;
        for (Map.Entry<Long, PendingCheckpoint> entry : new TreeMap<>(checkpoints).entrySet()) {
            if (entry.getKey() > checkpointId) {
                break;
            }
            latest = entry.getValue();
        }
        if (latest == null) {
            return;
        }
        if (!latest.allReceived()) {
            throw new IllegalStateException(
                    String.format(
                            "Restored checkpoint %d has partial PWC file info.", checkpointId));
        }
        saveCheckpoint(latest);
    }

    private void cleanupCommittedCheckpoints(long checkpointId) {
        checkpoints.keySet().removeIf(id -> id <= checkpointId);
        abortedCheckpoints.removeIf(id -> id <= checkpointId);
    }

    public void close() {
        registeredSubtasks.clear();
        checkpoints.clear();
        abortedCheckpoints.clear();
        recoveryCompleteSubtasks.clear();
    }
}
