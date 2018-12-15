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

package com.huaweicloud.dis.adapter.kafka.producer;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.model.DisProducerRecord;
import com.huaweicloud.dis.adapter.common.producer.DISProducer;
import com.huaweicloud.dis.adapter.common.producer.IDISProducer;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DISKafkaProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DISProducer.class);

    private IDISProducer disProducer;

    private DISConfig disConfig;

    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;


    public DISKafkaProducer(Map map) {
        this(map, null, null);
    }

    public DISKafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    public DISKafkaProducer(DISConfig disConfig) {
        this(disConfig, null, null);
    }

    public DISKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.newDisConfig(configs), keySerializer, valueSerializer);
    }

    public DISKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this((Map) properties, keySerializer, valueSerializer);
    }

    private DISKafkaProducer(DISConfig disConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.disConfig = disConfig;
        disProducer = new DISProducer(disConfig);
        init(keySerializer, valueSerializer);
    }

    private void init(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        if (keySerializer == null) {
            Class keySerializerClass = StringSerializer.class;
            String className = (String) disConfig.get("key.serializer");
            if (className != null) {
                try {
                    keySerializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                }
            }
            try {
                this.keySerializer = (Serializer<K>) keySerializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
            }
        }

        if (valueSerializer == null) {
            Class valueSerializerClass = StringSerializer.class;
            String className = (String) disConfig.get("value.serializer");
            if (className != null) {
                try {
                    valueSerializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                }
            }
            try {
                this.valueSerializer = (Serializer<V>) valueSerializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
            }
        }
    }


    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback callback) {

        String stream = record.topic();
        Integer partition = record.partition();
        Long timestamp = record.timestamp();
        String key = null;
        ByteBuffer value;
        K k = record.key();
        V v = record.value();

        if (v instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) v);
        } else {
            value = ByteBuffer.wrap(valueSerializer.serialize(stream, v));
        }

        if (k != null) {
            key = new String(keySerializer.serialize(stream, k));
        }

        Future<PutRecordsResult> putResultFuture = disProducer.send(new DisProducerRecord(stream, partition, timestamp, key, value));
        return new RecordMetadataFuture(stream, putResultFuture);

    }

    private RecordMetadata buildRecordMetadata(String streamName, PutRecordsResult result) {
        PutRecordsResultEntry resultEntry = result.getRecords().get(0);

        if (!StringUtils.isNullOrEmpty(resultEntry.getErrorCode())) {
            throw new RuntimeException(resultEntry.getErrorCode());
        }

        int partitionNum = getKafkaPartitionFromShardId(resultEntry.getPartitionId());

        RecordMetadata recordMetadata =
                new RecordMetadata(new TopicPartition(streamName, partitionNum), 0,
                        Long.parseLong(resultEntry.getSequenceNumber()), Record.NO_TIMESTAMP, -1, -1, -1);

        return recordMetadata;
    }

    private int getKafkaPartitionFromShardId(String shardId) {
        int zeroIndex = shardId.indexOf("0");

        int partitionNum = Integer.parseInt(shardId.substring(zeroIndex == -1 ? 0 : zeroIndex));
        return partitionNum;
    }

    @Override
    public void flush() {
        disProducer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        DescribeStreamResult describeStreamResult = disProducer.describeStream(topic);
        for (int i = 0; i < describeStreamResult.getWritablePartitionCount(); i++) {
            partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), null, null));
        }
        return partitionInfos;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {
        this.close(10000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        this.disProducer.close();
    }

    private class RecordMetadataFuture implements Future<RecordMetadata> {

        private Future<PutRecordsResult> future;
        private String streamName;

        public RecordMetadataFuture(String streamName, Future<PutRecordsResult> putResultFuture) {
            this.future = putResultFuture;
            this.streamName = streamName;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public RecordMetadata get()
                throws InterruptedException, ExecutionException {
            PutRecordsResult result = future.get();
            if (result == null) {
                return null;
            }
            return buildRecordMetadata(streamName, result);
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            PutRecordsResult result = future.get(timeout, unit);
            if (result == null) {
                return null;
            }
            return buildRecordMetadata(streamName, result);
        }

    }

}
