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
package com.cloud.dis.adapter.kafka.clients.consumer;

import com.cloud.dis.adapter.kafka.common.Metric;
import com.cloud.dis.adapter.kafka.common.MetricName;
import com.cloud.dis.adapter.kafka.common.PartitionInfo;
import com.cloud.dis.adapter.kafka.common.TopicPartition;
import com.cloud.dis.core.DISCredentials;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @see DISKafkaConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * @see DISKafkaConsumer#assignment()
     */
    public Set<TopicPartition> assignment();

    /**
     * @see DISKafkaConsumer#subscription()
     */
    public Set<String> subscription();

    /**
     * @see DISKafkaConsumer#subscribe(Collection)
     */
    public void subscribe(Collection<String> topics);

    /**
     * @see DISKafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see DISKafkaConsumer#assign(Collection)
     */
    public void assign(Collection<TopicPartition> partitions);

    /**
    * @see DISKafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
    */
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * @see DISKafkaConsumer#unsubscribe()
     */
    public void unsubscribe();

    /**
     * @see DISKafkaConsumer#poll(long)
     */
    public ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see DISKafkaConsumer#commitSync()
     */
    public void commitSync();

    /**
     * @see DISKafkaConsumer#commitSync(Map)
     */
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see DISKafkaConsumer#commitAsync()
     */
    public void commitAsync();

    /**
     * @see DISKafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    public void commitAsync(OffsetCommitCallback callback);

    /**
     * @see DISKafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * @see DISKafkaConsumer#seek(TopicPartition, long)
     */
    public void seek(TopicPartition partition, long offset);

    /**
     * @see DISKafkaConsumer#seekToBeginning(Collection)
     */
    public void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#seekToEnd(Collection)
     */
    public void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#position(TopicPartition)
     */
    public long position(TopicPartition partition);

    /**
     * @see DISKafkaConsumer#committed(TopicPartition)
     */
    public OffsetAndMetadata committed(TopicPartition partition);

    /**
     * @see DISKafkaConsumer#metrics()
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * @see DISKafkaConsumer#partitionsFor(String)
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see DISKafkaConsumer#listTopics()
     */
    public Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see DISKafkaConsumer#paused()
     */
    public Set<TopicPartition> paused();

    /**
     * @see DISKafkaConsumer#pause(Collection)
     */
    public void pause(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#resume(Collection)
     */
    public void resume(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#offsetsForTimes(Map)
     */
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * @see DISKafkaConsumer#beginningOffsets(Collection)
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#endOffsets(Collection)
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * @see DISKafkaConsumer#close()
     */
    public void close();

    /**
     * @see DISKafkaConsumer#close(long, TimeUnit)
     */
    public void close(long timeout, TimeUnit unit);

    /**
     * @see DISKafkaConsumer#wakeup()
     */
    public void wakeup();

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     * @param credentials new credentials
     */
    public void updateCredentials(DISCredentials credentials);

    public void updateAuthToken(String authToken);
}
