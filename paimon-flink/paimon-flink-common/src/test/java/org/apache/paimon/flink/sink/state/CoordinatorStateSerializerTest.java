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

package org.apache.paimon.flink.sink.state;

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compatibility tests for durable coordinator terminal coverage. */
public class CoordinatorStateSerializerTest {

    private final CoordinatorStateSerializer serializer = new CoordinatorStateSerializer();

    @Test
    public void testMixedTerminalRoundTrip() throws Exception {
        assertRoundTrip(new long[] {5, -1, 7});
    }

    @Test
    public void testAllTerminalRoundTrip() throws Exception {
        assertRoundTrip(new long[] {5, 7});
    }

    @Test
    public void testNoTerminalRoundTrip() throws Exception {
        assertRoundTrip(new long[] {-1, -1});
    }

    private void assertRoundTrip(long[] coverage) throws Exception {
        Map<String, byte[]> auxiliary = Collections.singletonMap("listener", new byte[] {1, 2});
        CoordinatorState state = new CoordinatorState("user", auxiliary, coverage);
        assertThat(serializer.getVersion()).isEqualTo(2);
        CoordinatorState restored = serializer.deserialize(2, serializer.serialize(state));
        assertThat(restored.getCommitUser()).isEqualTo("user");
        assertThat(restored.getCommitterStates().get("listener")).containsExactly(1, 2);
        assertThat(restored.getWriterParallelism()).isEqualTo(coverage.length);
        assertThat(restored.getTerminalCoveredBy()).containsExactly(coverage);
    }

    @Test
    public void testVersionOneHasNoTerminalCoverage() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("old-user");
        new MapSerializer<>(StringSerializer.INSTANCE, BytePrimitiveArraySerializer.INSTANCE)
                .serialize(Collections.singletonMap("aux", new byte[] {3}), out);
        CoordinatorState restored = serializer.deserialize(1, out.getCopyOfBuffer());
        assertThat(restored.getCommitUser()).isEqualTo("old-user");
        assertThat(restored.getCommitterStates().get("aux")).containsExactly(3);
        assertThat(restored.getWriterParallelism()).isZero();
        assertThat(restored.getTerminalCoveredBy()).isEmpty();
    }

    @Test
    public void testCoverageIsDefensivelyCopied() {
        long[] coverage = {5, -1};
        CoordinatorState state = new CoordinatorState("user", Collections.emptyMap(), coverage);
        coverage[0] = 9;
        state.getTerminalCoveredBy()[0] = 10;
        assertThat(state.getTerminalCoveredBy()).containsExactly(5, -1);
    }

    @Test
    public void testRejectsUnsupportedVersion() {
        assertThatThrownBy(() -> serializer.deserialize(3, new byte[0]))
                .isInstanceOf(IllegalStateException.class);
    }
}
