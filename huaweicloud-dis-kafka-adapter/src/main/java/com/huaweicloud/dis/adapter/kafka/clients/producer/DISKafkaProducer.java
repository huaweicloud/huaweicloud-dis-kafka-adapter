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

import com.huaweicloud.dis.Constants;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.model.DisProducerRecord;
import com.huaweicloud.dis.adapter.common.model.ProduceCallback;
import com.huaweicloud.dis.adapter.common.producer.DISProducer;
import com.huaweicloud.dis.adapter.common.producer.DisProducerConfig;
import com.huaweicloud.dis.adapter.common.producer.IDISProducer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.OffsetAndMetadata;
import com.huaweicloud.dis.adapter.kafka.common.Metric;
import com.huaweicloud.dis.adapter.kafka.common.MetricName;
import com.huaweicloud.dis.adapter.kafka.common.Node;
import com.huaweicloud.dis.adapter.kafka.common.PartitionInfo;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import com.huaweicloud.dis.adapter.kafka.common.serialization.Serializer;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
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

/**
 * A client that publishes records to the DIS.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put(ProducerConfig.PROPERTY_AK, &quot;YOU_AK&quot;);
 * props.put(ProducerConfig.PROPERTY_SK, &quot;YOU_AK&quot;);
 * props.put(ProducerConfig.PROPERTY_PROJECT_ID, &quot;YOU_PROJECT_ID&quot;);
 * props.put(ProducerConfig.PROPERTY_REGION_ID, &quot;cn-north-1&quot;);
 * props.put(ProducerConfig.PROPERTY_ENDPOINT, &quot;https://dis.cn-north-1.myhuaweicloud.com&quot;);
 * props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
 * props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer");
 * props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new DISKafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 50 millisecond. However this setting
 * would add 50 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link com.huaweicloud.dis.adapter.kafka.common.serialization.ByteArraySerializer} or
 * {@link com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * <p>
 */

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
            String className = (String) disConfig.get(DisProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
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
            String className = (String) disConfig.get(DisProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
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
        String streamId = record.streamId();
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

        ProduceCallback produceCallback = null;
        if (callback != null) {
            produceCallback = new ProduceCallback() {
                @Override
                public void onCompletion(PutRecordsResult result, Exception exception) {
                    if (exception == null) {
                        callback.onCompletion(buildRecordMetadata(stream, result), null);
                    } else {
                        callback.onCompletion(null, exception);
                    }
                }
            };
        }

        Future<PutRecordsResult> putResultFuture =
                disProducer.send(new DisProducerRecord(stream, streamId, partition, timestamp, key, value), produceCallback);
        return new RecordMetadataFuture(stream, putResultFuture);

    }

    private RecordMetadata buildRecordMetadata(String streamName, PutRecordsResult result) {
        PutRecordsResultEntry resultEntry = result.getRecords().get(0);

        // 流控时，不需要抛出异常
        if (!StringUtils.isNullOrEmpty(resultEntry.getErrorCode())
            && !Constants.ERROR_CODE_TRAFFIC_CONTROL_LIMIT.equals(
            resultEntry.getErrorCode())) {
            throw new RuntimeException(resultEntry.getErrorCode());
        }

        int partitionNum = getKafkaPartitionFromShardId(resultEntry.getPartitionId());

        RecordMetadata recordMetadata =
                new RecordMetadata(new TopicPartition(streamName, partitionNum), 0,
                        Long.parseLong(resultEntry.getSequenceNumber()), -1, -1, -1, -1);

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
        this.disProducer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        this.disProducer.close(timeout, unit);
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

    @Override
    public void initTransactions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTransaction() {
        throw new UnsupportedOperationException();
    }

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     *
     * @param credentials new credentials
     */
    public void updateCredentials(DISCredentials credentials) {
        disProducer.updateCredentials(credentials);
    }
}
