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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/** Replayable release signal for a specific terminal writer attempt. */
public class CommitSucceededEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final int subtask;
    private final int attemptNumber;
    private final long checkpointId;

    public CommitSucceededEvent(int subtask, int attemptNumber, long checkpointId) {
        if (subtask < 0
                || attemptNumber < 0
                || checkpointId < 0
                || checkpointId == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid commit success target or coverage");
        }
        this.subtask = subtask;
        this.attemptNumber = attemptNumber;
        this.checkpointId = checkpointId;
    }

    public int getSubtask() {
        return subtask;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getCheckpointId() {
        return checkpointId;
    }
}
