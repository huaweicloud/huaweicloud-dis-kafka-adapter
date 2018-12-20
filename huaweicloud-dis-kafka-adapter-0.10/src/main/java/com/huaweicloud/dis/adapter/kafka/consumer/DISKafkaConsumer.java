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

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.consumer.DISConsumer;
import com.huaweicloud.dis.adapter.common.consumer.DisConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.common.consumer.DisOffsetCommitCallback;
import com.huaweicloud.dis.adapter.common.model.DisOffsetAndMetadata;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;


public class DISKafkaConsumer<K, V> implements Consumer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DISKafkaConsumer.class);
    private static final Node[] EMPTY_NODES = null;

    private DISConsumer disConsumer;

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    public DISKafkaConsumer(Map configs) {
        this(newDisConfig(configs));
    }

    public DISKafkaConsumer(Map<String, Object> configs,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        this(newDisConfig(configs), keyDeserializer, valueDeserializer);
    }

    public DISKafkaConsumer(Properties properties) {
        this((Map) properties);
    }

    public DISKafkaConsumer(Properties properties,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        this((Map) properties, keyDeserializer, valueDeserializer);
    }

    public DISKafkaConsumer(DISConfig disConfig) {
        this(disConfig, null, null);
    }

    public DISKafkaConsumer(DISConfig disConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        disConsumer = new DISConsumer(disConfig);
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        if (keyDeserializer == null) {
            Class keyDeserializerClass = StringDeserializer.class;
            String className = (String) disConfig.get("key.deserializer");
            if (className != null) {
                try {
                    keyDeserializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                    return;
                }
            }
            try {
                this.keyDeserializer = (Deserializer<K>) keyDeserializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
                return;
            }
        }

        if (valueDeserializer == null) {
            Class valueDeserializerClass = StringDeserializer.class;
            String className = (String) disConfig.get("value.deserializer");
            if (className != null) {
                try {
                    valueDeserializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                    return;
                }
            }
            try {
                this.valueDeserializer = (Deserializer<V>) valueDeserializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
                return;
            }
        }
        log.debug("create DISKafkaConsumer successfully");
    }

    private static DISConfig newDisConfig(Map map) {
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(map);
        return disConfig;
    }

    private static Set<TopicPartition> convertStreamPartition(Set<StreamPartition> streamPartitions) {
        Set<TopicPartition> partitions = new HashSet<>();
        if (streamPartitions != null) {
            for (StreamPartition partition : streamPartitions) {
                partitions.add(new TopicPartition(partition.stream(), partition.partition()));
            }
        }
        return partitions;
    }

    private static Collection<TopicPartition> convertStreamPartition(Collection<StreamPartition> streamPartitions) {
        Collection<TopicPartition> partitions = new HashSet<>();
        if (streamPartitions != null) {
            for (StreamPartition partition : streamPartitions) {
                partitions.add(new TopicPartition(partition.stream(), partition.partition()));
            }
        }
        return partitions;
    }

    private static Set<StreamPartition> convertTopicPartition(Set<TopicPartition> topicPartitions) {
        Set<StreamPartition> partitions = new HashSet<>();
        if (topicPartitions != null) {
            for (TopicPartition partition : topicPartitions) {
                partitions.add(new StreamPartition(partition.topic(), partition.partition()));
            }
        }
        return partitions;
    }

    private static Collection<StreamPartition> convertTopicPartition(Collection<TopicPartition> topicPartitions) {
        Collection<StreamPartition> partitions = new HashSet<>();
        if (topicPartitions != null) {
            for (TopicPartition partition : topicPartitions) {
                partitions.add(new StreamPartition(partition.topic(), partition.partition()));
            }
        }
        return partitions;
    }

    @Override
    public Set<TopicPartition> assignment() {
        Set<StreamPartition> streamPartitions = disConsumer.assignment();
        return convertStreamPartition(streamPartitions);
    }

    @Override
    public Set<String> subscription() {
        return disConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(collection, new DisConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<StreamPartition> partitions) {
                if (consumerRebalanceListener != null) {
                    consumerRebalanceListener.onPartitionsRevoked(convertStreamPartition(partitions));
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<StreamPartition> partitions) {
                if (consumerRebalanceListener != null) {
                    consumerRebalanceListener.onPartitionsAssigned(convertStreamPartition(partitions));
                }
            }
        });
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        disConsumer.assign(convertTopicPartition(partitions));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(pattern, new DisConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<StreamPartition> partitions) {
                if (consumerRebalanceListener != null) {
                    consumerRebalanceListener.onPartitionsRevoked(convertStreamPartition(partitions));
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<StreamPartition> partitions) {
                if (consumerRebalanceListener != null) {
                    consumerRebalanceListener.onPartitionsAssigned(convertStreamPartition(partitions));
                }
            }
        });
    }

    @Override
    public void unsubscribe() {
        disConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> res = new HashMap<>();
        Map<StreamPartition, List<Record>> records = disConsumer.poll(timeout);
        for (Map.Entry<StreamPartition, List<Record>> entry : records.entrySet()) {
            TopicPartition partition = new TopicPartition(entry.getKey().stream(), entry.getKey().partition());
            for (Record record : entry.getValue()) {
                K key = record.getPartitionKey() == null ? null
                        : keyDeserializer.deserialize(entry.getKey().stream(), record.getPartitionKey().getBytes());
                V value = valueDeserializer.deserialize(entry.getKey().stream(), record.getData().array());
                ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<K, V>(entry.getKey().stream(),
                        entry.getKey().partition(), Long.valueOf(record.getSequenceNumber()), record.getTimestamp(),
                        TimestampType.forName(record.getTimestampType()), 0L, 0, 0, key, value);
                res.putIfAbsent(partition, new ArrayList<>());
                res.get(partition).add(consumerRecord);
            }
        }
        return new ConsumerRecords<K, V>(res);
    }

    @Override
    public void commitSync() {
        disConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitAsync(offsets, null);
    }

    @Override
    public void commitAsync() {
        disConsumer.commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        disConsumer.commitAsync(new DisOffsetCommitCallback() {
            @Override
            public void onComplete(Map<StreamPartition, DisOffsetAndMetadata> offsets, Exception exception) {
                Map<TopicPartition, OffsetAndMetadata> results = null;
                if (offsets != null) {
                    results = new HashMap<>();
                    for (Map.Entry<StreamPartition, DisOffsetAndMetadata> entry : offsets.entrySet()) {
                        results.put(new TopicPartition(entry.getKey().stream(), entry.getKey().partition()), new OffsetAndMetadata(entry.getValue().offset(), entry.getValue().metadata()));
                    }
                }
                if (offsetCommitCallback != null) {
                    offsetCommitCallback.onComplete(results, exception);
                }
            }
        });
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> map, OffsetCommitCallback offsetCommitCallback) {
        Map<StreamPartition, DisOffsetAndMetadata> tmp = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
            tmp.put(new StreamPartition(entry.getKey().topic(), entry.getKey().partition()), new DisOffsetAndMetadata(entry.getValue().offset(), entry.getValue().metadata()));
        }
        disConsumer.commitAsync(tmp, new DisOffsetCommitCallback() {
            @Override
            public void onComplete(Map<StreamPartition, DisOffsetAndMetadata> offsets, Exception exception) {
                Map<TopicPartition, OffsetAndMetadata> results = null;
                if (offsets != null) {
                    results = new HashMap<>();
                    for (Map.Entry<StreamPartition, DisOffsetAndMetadata> entry : offsets.entrySet()) {
                        results.put(new TopicPartition(entry.getKey().stream(), entry.getKey().partition()), new OffsetAndMetadata(entry.getValue().offset(), entry.getValue().metadata()));
                    }
                }
                if (offsetCommitCallback != null) {
                    offsetCommitCallback.onComplete(results, exception);
                }
            }
        });
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        disConsumer.seek(new StreamPartition(partition.topic(), partition.partition()), offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        disConsumer.seekToBeginning(convertTopicPartition(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        disConsumer.seekToEnd(convertTopicPartition(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return disConsumer.position(new StreamPartition(partition.topic(), partition.partition()));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        DisOffsetAndMetadata disOffsetAndMetadata = disConsumer.committed(new StreamPartition(partition.topic(), partition.partition()));
        if (disOffsetAndMetadata != null) {
            return new OffsetAndMetadata(disOffsetAndMetadata.offset(), disOffsetAndMetadata.metadata());
        }
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        DescribeStreamResult describeStreamResult = disConsumer.describeStream(topic);
        for (int i = 0; i < describeStreamResult.getReadablePartitionCount(); i++) {
            partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), EMPTY_NODES, EMPTY_NODES));
        }
        return partitionInfos;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        List<DescribeStreamResult> results = disConsumer.listStreams();
        if (results == null) {
            return null;
        }
        Map<String, List<PartitionInfo>> map = new HashMap<>();
        for (DescribeStreamResult describeStreamResult : results) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (int i = 0; i < describeStreamResult.getReadablePartitionCount(); i++) {
                partitionInfos.add(new PartitionInfo(describeStreamResult.getStreamName(), i, Node.noNode(), EMPTY_NODES, EMPTY_NODES));
            }
        }
        return map;
    }

    @Override
    public Set<TopicPartition> paused() {
        return convertStreamPartition(disConsumer.paused());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        disConsumer.pause(convertTopicPartition(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        disConsumer.resume(convertTopicPartition(partitions));
    }

    @Override
    public void close() {
        disConsumer.close();
    }

    @Override
    public void wakeup() {
        disConsumer.wakeup();
    }
}
