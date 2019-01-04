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
import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig;
import com.huaweicloud.dis.adapter.common.consumer.DisNoOpDisConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.common.consumer.DisOffsetAndTimestamp;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.adapter.kafka.ConvertUtils;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class DISKafkaConsumer<K, V> implements Consumer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DISKafkaConsumer.class);
    private static final Node[] EMPTY_NODES = new Node[0];

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
            String className = (String) disConfig.get(DisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
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
            String className = (String) disConfig.get(DisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
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

    @Override
    public Set<TopicPartition> assignment() {
        return ConvertUtils.convert2TopicPartitionSet(disConsumer.assignment());
    }

    @Override
    public Set<String> subscription() {
        return disConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(collection, ConvertUtils.convert2DisConsumerRebalanceListener(consumerRebalanceListener));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        disConsumer.subscribe(topics, new DisNoOpDisConsumerRebalanceListener());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        disConsumer.assign(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(pattern, ConvertUtils.convert2DisConsumerRebalanceListener(consumerRebalanceListener));
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
        disConsumer.commitSync(ConvertUtils.convert2DisOffsetAndMetadataMap(offsets));
    }

    @Override
    public void commitAsync() {
        disConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        disConsumer.commitAsync(ConvertUtils.convert2DisOffsetCommitCallback(offsetCommitCallback));
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback offsetCommitCallback) {
        disConsumer.commitAsync(ConvertUtils.convert2DisOffsetAndMetadataMap(offsets),
                ConvertUtils.convert2DisOffsetCommitCallback(offsetCommitCallback));
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        disConsumer.seek(ConvertUtils.convert2StreamPartition(partition), offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        disConsumer.seekToBeginning(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        disConsumer.seekToEnd(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return disConsumer.position(ConvertUtils.convert2StreamPartition(partition));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return ConvertUtils.convert2OffsetAndMetadata(
                disConsumer.committed(ConvertUtils.convert2StreamPartition(partition)));
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
            map.put(describeStreamResult.getStreamName(), partitionInfos);
        }
        return map;
    }

    @Override
    public Set<TopicPartition> paused() {
        return ConvertUtils.convert2TopicPartitionSet(disConsumer.paused());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        disConsumer.pause(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        disConsumer.resume(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void close() {
        disConsumer.close();
    }

    @Override
    public void wakeup() {
        disConsumer.wakeup();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        close();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map) {
        Map<StreamPartition, DisOffsetAndTimestamp> offsets = disConsumer.offsetsForTimes(ConvertUtils.convert2StreamPartitionLongMap(map));
        Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>();
        for (Map.Entry<StreamPartition, DisOffsetAndTimestamp> entry : offsets.entrySet()) {
            results.put(ConvertUtils.convert2TopicPartition(entry.getKey()), new OffsetAndTimestamp(entry.getValue().offset(), entry.getValue().timestamp()));
        }
        return results;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        return ConvertUtils.convert2TopicPartitionLongMap(
                disConsumer.beginningOffsets(ConvertUtils.convert2StreamPartitionCollection(collection)));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        return ConvertUtils.convert2TopicPartitionLongMap(
                disConsumer.endOffsets(ConvertUtils.convert2StreamPartitionCollection(collection)));
    }

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     *
     * @param credentials new credentials
     */
    public void updateCredentials(DISCredentials credentials) {
        disConsumer.updateCredentials(credentials);
    }
}
