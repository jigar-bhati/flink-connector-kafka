/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.split;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaPartitionSplitSerializer}. */
public class KafkaPartitionSplitSerializerTest {

    @Test
    public void testSerializer() throws IOException {
        String topic = "topic";
        Long offsetZero = 0L;
        Long normalOffset = 1L;
        TopicPartition topicPartition = new TopicPartition(topic, 1);
        List<Long> stoppingOffsets =
                Lists.newArrayList(KafkaPartitionSplit.COMMITTED_OFFSET, offsetZero, normalOffset);
        KafkaPartitionSplitSerializer splitSerializer = new KafkaPartitionSplitSerializer();
        for (Long stoppingOffset : stoppingOffsets) {
            KafkaPartitionSplit kafkaPartitionSplit =
                    new KafkaPartitionSplit(
                            topicPartition, 0, stoppingOffset, OffsetResetStrategy.EARLIEST);
            byte[] serialize = splitSerializer.serialize(kafkaPartitionSplit);
            KafkaPartitionSplit deserializeSplit =
                    splitSerializer.deserialize(splitSerializer.getVersion(), serialize);
            assertThat(deserializeSplit).isEqualTo(kafkaPartitionSplit);
            assertThat(deserializeSplit.getStartingOffsetResetStrategy())
                    .contains(OffsetResetStrategy.EARLIEST);
        }
    }

    @Test
    public void testDeserializeVersionZeroUsesRecoveryResetStrategy() throws IOException {
        String topic = "topic";
        TopicPartition topicPartition = new TopicPartition(topic, 1);
        KafkaPartitionSplitSerializer splitSerializer = new KafkaPartitionSplitSerializer();
        KafkaPartitionSplit kafkaPartitionSplit = new KafkaPartitionSplit(topicPartition, 0, 1);

        byte[] serialized = serializeVersionZero(kafkaPartitionSplit);
        KafkaPartitionSplit deserializeSplit = splitSerializer.deserialize(0, serialized);

        assertThat(deserializeSplit.getStartingOffsetResetStrategy()).isEmpty();
    }

    @Test
    public void testSplitStateDropsStartingOffsetResetStrategy() {
        KafkaPartitionSplit split =
                new KafkaPartitionSplit(
                        new TopicPartition("topic", 1),
                        10,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET,
                        OffsetResetStrategy.EARLIEST);

        KafkaPartitionSplit checkpointSplit =
                new KafkaPartitionSplitState(split).toKafkaPartitionSplit();

        assertThat(checkpointSplit.getStartingOffset()).isEqualTo(10);
        assertThat(checkpointSplit.getStartingOffsetResetStrategy()).isEmpty();
    }

    @Test
    public void testRejectsUnsupportedVersion() throws IOException {
        KafkaPartitionSplitSerializer splitSerializer = new KafkaPartitionSplitSerializer();
        KafkaPartitionSplit split = new KafkaPartitionSplit(new TopicPartition("topic", 1), 0, 1);

        assertThatThrownBy(
                        () ->
                                splitSerializer.deserialize(
                                        splitSerializer.getVersion() + 1,
                                        splitSerializer.serialize(split)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("only supports version up to");
    }

    private byte[] serializeVersionZero(KafkaPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getTopic());
            out.writeInt(split.getPartition());
            out.writeLong(split.getStartingOffset());
            out.writeLong(split.getStoppingOffset().orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET));
            out.flush();
            return baos.toByteArray();
        }
    }
}
