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
package com.huaweicloud.dis.adapter.kafka.clients.producer;

/**
 * A key/value pair to be sent to DIS. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 * <p>
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by DIS depends on the timestamp type configured for
 * the topic.
 * <li>
 * If the topic is configured to use {@link com.huaweicloud.dis.adapter.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * </li>
 * <li>
 * If the topic is configured to use {@link com.huaweicloud.dis.adapter.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * </li>
 * <p>
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 */
public class ProducerRecord<K, V> {

    private final String topic;
    private final String streamId;
    private final Integer partition;
    // comment by adapter
    // private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param streamId The ID of stream the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record
     * @param key The key that will be included in the record
     * @param value The record contents
     * // @param headers the headers that will be included in the record
     */
    public ProducerRecord(String topic, String streamId, Integer partition, Long timestamp, K key, V value) {
        if (topic == null && streamId == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        this.topic = topic;
        this.streamId = streamId;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        // comment by adapter
        // this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, null, partition, timestamp, key, value);
    }

    // comment by adapter
    // /**
    //  * Creates a record with a specified timestamp to be sent to a specified topic and partition
    //  *
    //  * @param topic The topic the record will be appended to
    //  * @param partition The partition to which the record should be sent
    //  * @param timestamp The timestamp of the record
    //  * @param key The key that will be included in the record
    //  * @param value The record contents
    //  */
    // public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
    //     this(topic, partition, timestamp, key, value, null);
    // }

    // comment by adapter
    // /**
    //  * Creates a record to be sent to a specified topic and partition
    //  *
    //  * @param topic The topic the record will be appended to
    //  * @param partition The partition to which the record should be sent
    //  * @param key The key that will be included in the record
    //  * @param value The record contents
    //  * @param headers The headers that will be included in the record
    //  */
    // public ProducerRecord(String topic, Integer partition, K key, V value,  Iterable<Header> headers) {
    //     this(topic, partition, null, key, value, headers);
    // }
    
    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, null, partition, null, key, value);
    }
    
    /**
     * Create a record to be sent to DIS
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, null, key, value);
    }

    /**
     * Create a record with streamId to be sent to DIS
     *
     * @param topic The topic the record will be appended to
     * @param streamId The ID of stream the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, String streamId, K key, V value) {
        this(topic, streamId, null, null, key, value);
    }
    
    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, null, value);
    }

    /**
     * @return The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * @return The streamId this record is being sent to
     */
    public String streamId() {
        return streamId;
    }

    // comment by adapter
    // /**
    //  * @return The headers
    //  */
    // public Headers headers() {
    //     return headers;
    // }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }

    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        // comment by adapter
        // String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        if (key != null ? !key.equals(that.key) : that.key != null) 
            return false;
        else if (partition != null ? !partition.equals(that.partition) : that.partition != null) 
            return false;
        else if (topic != null ? !topic.equals(that.topic) : that.topic != null) 
            return false;
        // comment by adapter
        // else if (headers != null ? !headers.equals(that.headers) : that.headers != null)
        //     return false;
        else if (value != null ? !value.equals(that.value) : that.value != null) 
            return false;
        else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        // comment by adapter
        // result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
