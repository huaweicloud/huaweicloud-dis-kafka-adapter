/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloud.dis.adapter.kafka.clients.producer;

import com.cloud.dis.adapter.kafka.common.TopicPartition;

/**
 * The metadata for a record that has been acknowledged by the server
 */
public final class RecordMetadata {

    /**
     * Partition value for record without partition assigned
     */
    public static final int UNKNOWN_PARTITION = -1;

    private final long offset;
    // The timestamp of the message.
    // If LogAppendTime is used for the topic, the timestamp will be the timestamp returned by the broker.
    // If CreateTime is used for the topic, the timestamp is the timestamp in the corresponding ProducerRecord if the
    // user provided one. Otherwise, it will be the producer local time when the producer record was handed to the
    // producer.
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;

    private volatile Long checksum;

    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset, long timestamp,
                          Long checksum, int serializedKeySize, int serializedValueSize) {
        // ignore the relativeOffset if the base offset is -1,
        // since this indicates the offset is unknown
        this.offset = baseOffset == -1 ? baseOffset : baseOffset + relativeOffset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    /**
     * @deprecated As of 0.11.0. Use @{@link RecordMetadata#RecordMetadata(TopicPartition, long, long, long, Long, int, int)}.
     */
    @Deprecated
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset, long timestamp,
                          long checksum, int serializedKeySize, int serializedValueSize) {
        this(topicPartition, baseOffset, relativeOffset, timestamp, Long.valueOf(checksum), serializedKeySize,
                serializedValueSize);
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * The timestamp of the record in the topic/partition.
     */
    public long timestamp() {
        return timestamp;
    }

    // comment by adapter
    // /**
    //  * The checksum (CRC32) of the record.
    //  *
    //  * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
    //  *             computed checksum may not match what was stored on the broker, or what will be returned to the consumer.
    //  *             It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
    //  *             message format v2 does not include a record-level checksum (for performance, the record checksum
    //  *             was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
    //  *             the record timestamp, serialized key size, and serialized value size is returned instead, but
    //  *             this should not be depended on for end-to-end reliability.
    //  */
    // @Deprecated
    // public long checksum() {
    //     if (checksum == null)
    //         // The checksum is null only for message format v2 and above, which do not have a record-level checksum.
    //         this.checksum = DefaultRecord.computePartialChecksum(timestamp, serializedKeySize, serializedValueSize);
    //     return this.checksum;
    // }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned
     * size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
