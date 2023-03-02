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

import com.cloud.dis.adapter.kafka.clients.consumer.OffsetAndMetadata;
import com.cloud.dis.adapter.kafka.common.Metric;
import com.cloud.dis.adapter.kafka.common.MetricName;
import com.cloud.dis.adapter.kafka.common.PartitionInfo;
import com.cloud.dis.adapter.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The interface for the {@link DISKafkaProducer}
 * @see DISKafkaProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * See {@link DISKafkaProducer#initTransactions()}
     */
    void initTransactions();

    /**
     * See {@link DISKafkaProducer#beginTransaction()}
     */
    void beginTransaction();

    /**
     * See {@link DISKafkaProducer#sendOffsetsToTransaction(Map, String)}
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  String consumerGroupId);

    /**
     * See {@link DISKafkaProducer#commitTransaction()}
     */
    void commitTransaction();

    /**
     * See {@link DISKafkaProducer#abortTransaction()}
     */
    void abortTransaction();

    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     *
     * @param record The record to send
     * @return A future which will eventually contain the response information
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * Return a map of metrics maintained by the producer
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * Close this producer
     */
    public void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);

}
