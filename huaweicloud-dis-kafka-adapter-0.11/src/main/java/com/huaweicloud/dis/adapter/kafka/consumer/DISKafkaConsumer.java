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

package com.huaweicloud.dis.adapter.kafka.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.huaweicloud.dis.adapter.kafka.AbstractAdapter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.*;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.request.ListStreamsRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.ListStreamsResult;


public class DISKafkaConsumer<K, V> extends AbstractAdapter implements Consumer<K, V>
{
    private static final Logger log = LoggerFactory.getLogger(DISKafkaConsumer.class);
    private static final Node[] EMPTY_NODES = null;
    private static final long NO_CURRENT_THREAD = -1L;
    private Coordinator coordinator;
    private SubscriptionState subscriptions;
    private boolean closed = false;
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refcount = new AtomicInteger(0);

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private String clientId;
    private String groupId;
    private Fetcher fetcher;
    ConcurrentHashMap<TopicPartition,PartitionCursor> nextIterators;

    public DISKafkaConsumer(Map configs){this(newDisConfig(configs));}
    public DISKafkaConsumer(Map<String, Object> configs,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer){this(newDisConfig(configs),keyDeserializer,valueDeserializer);}
    public DISKafkaConsumer(Properties properties){this((Map)properties);}
    public DISKafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer){this((Map)properties,keyDeserializer,valueDeserializer);}
    public DISKafkaConsumer(DISConfig disConfig) {
        this(disConfig, null, null);
    }
    public DISKafkaConsumer(DISConfig disConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
    {
        super(disConfig);
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.clientId = disConfig.get("client.id","consumer-"+ UUID.randomUUID());
        this.groupId = disConfig.get("group.id","");
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(disConfig.get("auto.offset.reset","LATEST").toUpperCase());
        this.subscriptions = new SubscriptionState(offsetResetStrategy);
        this.nextIterators = new ConcurrentHashMap<>();
        if (keyDeserializer == null)
        {
            Class keyDeserializerClass = StringDeserializer.class;
            String className = (String)disConfig.get("key.deserializer");
            if (className != null)
            {
                try
                {
                    keyDeserializerClass = Class.forName(className);
                }
                catch (ClassNotFoundException e)
                {
                    log.error(e.getMessage());
                    return;
                }
            }
            try
            {
                this.keyDeserializer = (Deserializer<K>)keyDeserializerClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                log.error(e.getMessage());
                return;
            }
        }
        
        if (valueDeserializer == null)
        {
            Class valueDeserializerClass = StringDeserializer.class;
            String className = (String)disConfig.get("value.deserializer");
            if (className != null)
            {
                try
                {
                    valueDeserializerClass = Class.forName(className);
                }
                catch (ClassNotFoundException e)
                {
                    log.error(e.getMessage());
                    return;
                }
            }
            try
            {
                this.valueDeserializer = (Deserializer<V>)valueDeserializerClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                log.error(e.getMessage());
                return;
            }
        }
        boolean autoCommitEnabled = disConfig.getBoolean("enable.auto.commit",true);
        long autoCommitIntervalMs = Long.valueOf(disConfig.get("auto.commit.interval.ms","5000"));
        this.coordinator = new Coordinator(this.disClient,
                this.clientId,
                this.groupId,
                this.subscriptions,
                autoCommitEnabled,
                autoCommitIntervalMs,
                this.nextIterators,
                disConfig);
        this.fetcher = new Fetcher(disConfig,
                this.subscriptions,
                this.coordinator,
                this.nextIterators);
        log.info("create DISKafkaConsumer successfully");
    }

    private static DISConfig newDisConfig(Map map)
    {
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(map);
        return disConfig;
    }
    @Override
    public Set<TopicPartition> assignment() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.assignedPartitions()));
        } finally {
            release();
        }
    }

    @Override
    public Set<String> subscription() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquire();
        try {
            if (topics.isEmpty()) {
                this.unsubscribe();
            } else {
                log.debug("Subscribed to topic(s): {}", org.apache.kafka.common.utils.Utils.join(topics, ", "));
                this.subscriptions.subscribe(topics, listener);
            }
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics)
    {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions)
    {
        acquire();
        try {
            log.debug("Subscribed to partition(s): {}", org.apache.kafka.common.utils.Utils.join(partitions, ", "));
            this.subscriptions.assignFromUser(partitions);
         //   this.subscriptions.commitsRefreshed();
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback)
    {
        acquire();
        try {
            log.debug("Subscribed to pattern: {}", pattern);
            this.subscriptions.subscribe(pattern, callback);
        } finally {
            release();
        }
        
    }

    @Override
    public void unsubscribe()
    {
        acquire();
        try {
            log.debug("Unsubscribed all topics or patterns and assigned partitions");
            this.subscriptions.unsubscribe();
            coordinator.maybeLeaveGroup();
        } finally {
            release();
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout)
    {
        acquire();
        try {
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            if(subscriptions.partitionsAutoAssigned())
            {
                coordinator.ensureGroupStable();
            }

            fetcher.sendFetchRequests();
            Map<TopicPartition, List<ConsumerRecord<K, V>>> res = new HashMap<>();
            Map<TopicPartition, List<Record>> records =  fetcher.fetchRecords(timeout);
            coordinator.executeDelayedTask();
            for(Map.Entry<TopicPartition, List<Record>> entry: records.entrySet())
            {
                for(Record record: entry.getValue())
                {
                    K key = record.getPartitionKey() == null ? null
                        : keyDeserializer.deserialize(entry.getKey().topic(), record.getPartitionKey().getBytes());
                    V value = valueDeserializer.deserialize(entry.getKey().topic(),record.getData().array());
                    ConsumerRecord<K,V> consumerRecord = new ConsumerRecord<K, V>(entry.getKey().topic(),
                            entry.getKey().partition(),Long.valueOf(record.getSequenceNumber()),record.getTimestamp(),
                            TimestampType.forName(record.getTimestampType()),0L,0,0,key,value);
                    res.putIfAbsent(entry.getKey(),new ArrayList<>());
                    res.get(entry.getKey()).add(consumerRecord);
                }
            }
            return new ConsumerRecords<K, V>(res);
        } finally {
            release();
        }
    }

    @Override
    public void commitSync()
    {
        acquire();
        try {
            commitSync(subscriptions.allConsumed());
        } finally {
            release();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)
    {
        acquire();
        try {
            coordinator.commitSync(offsets);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync()
    {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback)
    {
        acquire();
        try {
            commitAsync(subscriptions.allConsumed(), callback);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback)
    {
        acquire();
        try {
            log.debug("Committing offsets: {} ", offsets);
            coordinator.commitAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset)
    {
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        acquire();
        try {
            log.debug("Seeking to offset {} for partition {}", offset, partition);
            this.subscriptions.seek(partition, offset);
            nextIterators.remove(partition);
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions)
    {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (TopicPartition tp : parts) {
                log.debug("Seeking to beginning of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);
            }
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions)
    {
        acquire();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (TopicPartition tp : parts) {
                log.debug("Seeking to end of partition {}", tp);
                subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);
            }
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition)
    {
        acquire();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
            Set<TopicPartition> needUpdatePositionPartition = new HashSet<>();
            for(TopicPartition part:this.subscriptions.assignedPartitions())
            {
                if(this.subscriptions.position(part) == null)
                {
                    needUpdatePositionPartition.add(part);
                }
            }
            if (!needUpdatePositionPartition.isEmpty()) {
                coordinator.updateFetchPositions(needUpdatePositionPartition);
            }
            return this.subscriptions.position(partition);
        } finally {
            release();
        }
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition)
    {
        acquire();
        try {
            OffsetAndMetadata committed;
            if (subscriptions.isAssigned(partition)) {
                committed = this.subscriptions.committed(partition);
                if (committed == null) {
                    coordinator.refreshCommittedOffsetsIfNeeded();
                    committed = this.subscriptions.committed(partition);
                }
            } else {
               Map<TopicPartition, OffsetAndMetadata> offsets = coordinator.fetchCommittedOffsets(Collections.singleton(partition));
               committed = offsets.get(partition);
            }
            return committed;
        } finally {
            release();
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic)
    {
        acquire();
        try
        {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName(topic);
            describeStreamRequest.setLimitPartitions(1);
            DescribeStreamResult describeStreamResult = disClient.describeStream(describeStreamRequest);
            for (int i = 0; i < describeStreamResult.getReadablePartitionCount(); i++)
            {
                partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), EMPTY_NODES, EMPTY_NODES));
            }
            return partitionInfos;
        }
        finally
        {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics()
    {
        acquire();
        Map<String, List<PartitionInfo>> res = new HashMap<>();
        try {
            int limit = 100;
            String startStreamName = "";
            while (true)
            {
                ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
                listStreamsRequest.setLimit(limit);
                listStreamsRequest.setExclusivetartStreamName(startStreamName);
                ListStreamsResult listStreamsResult = disClient.listStreams(listStreamsRequest);
                if(listStreamsResult == null || listStreamsResult.getStreamNames() == null)
                {
                    break;
                }
                List<String > topics = listStreamsResult.getStreamNames();
                for(String topic: topics)
                {
                    List<PartitionInfo> partitionInfos = partitionsFor(topic);
                    res.put(topic, partitionInfos);
                }
                if(!listStreamsResult.getHasMoreStreams())
                {
                    break;
                }
                startStreamName = topics.get(topics.size()-1);
            }
        }finally {
            release();
        }
        return res;
    }

    @Override
    public Set<TopicPartition> paused()
    {
        acquire();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions)
    {
        acquire();
        try {
            for (TopicPartition partition: partitions) {
                log.debug("Pausing partition {}", partition);
                subscriptions.pause(partition);
                fetcher.pause(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions)
    {
        acquire();
        try {
            for (TopicPartition partition: partitions) {
                log.debug("Resuming partition {}", partition);
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        close();
    }

    @Override
    public void wakeup()
    {
        // TODO Auto-generated method stub
        
    }

    private void acquire() {
        ensureNotClosed();
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("DisConsumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }
    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }
}
