/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.kafka.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.*;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.producer.DISProducer;
import com.huaweicloud.dis.adapter.kafka.AbstractAdapter;

public class DISKafkaProducer<K, V> extends AbstractAdapter implements Producer<K, V>
{
    
    private static final Logger log = LoggerFactory.getLogger(DISKafkaProducer.class);
    
    protected DISProducer producer;
    
    public DISKafkaProducer()
    {
    }
    
    public DISKafkaProducer(Map map)
    {
        this(map, null, null);
    }
    
    public DISKafkaProducer(Properties properties)
    {
        this(properties, null, null);
    }
    
    public DISKafkaProducer(DISConfig disConfig)
    {
        this(disConfig, null, null);
    }
    
    public DISKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        super(configs);
        init(keySerializer, valueSerializer);
    }
    
    public DISKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        super(properties);
        init(keySerializer, valueSerializer);
    }
    
    private DISKafkaProducer(DISConfig disConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        super(disConfig);
        init(keySerializer, valueSerializer);
    }
    
    private void init(Serializer<K> keySerializer, Serializer<V> valueSerializer)
    {
        producer = new DISProducer(config);
        
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        
        if (keySerializer == null)
        {
            Class keySerializerClass = StringSerializer.class;
            String className = (String)config.get("key.serializer");
            if (className != null)
            {
                try
                {
                    keySerializerClass = Class.forName(className);
                }
                catch (ClassNotFoundException e)
                {
                    log.error(e.getMessage());
                }
            }
            try
            {
                this.keySerializer = (Serializer<K>)keySerializerClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                log.error(e.getMessage());
            }
        }
        
        if (valueSerializer == null)
        {
            Class valueSerializerClass = StringSerializer.class;
            String className = (String)config.get("value.serializer");
            if (className != null)
            {
                try
                {
                    valueSerializerClass = Class.forName(className);
                }
                catch (ClassNotFoundException e)
                {
                    log.error(e.getMessage());
                }
            }
            try
            {
                this.valueSerializer = (Serializer<V>)valueSerializerClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                log.error(e.getMessage());
            }
        }
    }

    private String clientId;
    private Partitioner partitioner;
    private int maxRequestSize;
    private long totalMemorySize;
    private Metadata metadata;
    private RecordAccumulator accumulator;
    private Sender sender;
    private Metrics metrics;
    private Thread ioThread;
    private CompressionType compressionType;
    private Sensor errors;
    private Time time;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private ProducerConfig producerConfig;
    private long maxBlockTimeMs;
    private int requestTimeoutMs;
    private ProducerInterceptors<K, V> interceptors;
    
    
    
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record)
    {
        return send(record, null);
    }
    
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback callback)
    {
        
        String streamName = record.topic();
        K k = record.key();
        V v = record.value();
        record.partition();
        record.timestamp();
        
        
        PutRecordsRequest request = new PutRecordsRequest();
        request.setStreamName(streamName);
        
        List<PutRecordsRequestEntry> recordEntrys = new ArrayList<PutRecordsRequestEntry>();
        
        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        
        if(v instanceof byte[]){
            entry.setData(ByteBuffer.wrap((byte[])v));
        }else{
            entry.setData(ByteBuffer.wrap(valueSerializer.serialize(streamName, v)));
        }
        
        if (record.partition() != null)
        {
        	entry.setPartitionId(record.partition().toString());
        }
        
        if (k != null) {
        	entry.setPartitionKey(new String(keySerializer.serialize(streamName, k)));
		}

		if(record.timestamp()!=null)
        {
            entry.setTimestamp(record.timestamp());
        }
                
        recordEntrys.add(entry);
        
        request.setRecords(recordEntrys);
        
        Future<PutRecordsResult> putResultFuture = null;
        try
        {
            putResultFuture = producer.putRecordsAsync(request, new AsyncHandler<PutRecordsResult>()
                {
                    @Override
                    public void onSuccess(PutRecordsResult result)
                    {
                        if (callback != null)
                        {
                            RecordMetadata recordMetadata = buildRecordMetadata(streamName, result);
                            
                            callback.onCompletion(recordMetadata, null);
                        }
                    }
                    
                    @Override
                    public void onError(Exception exception)
                    {
                        if (callback != null)
                        {
                            callback.onCompletion(null, exception);
                        }
                    }
                });
        }
        catch (InterruptedException e1)
        {
            throw new RuntimeException(e1);
        }
        
        return new RecordMetadataFuture(streamName, putResultFuture);
        
    }

    private RecordMetadata buildRecordMetadata(String streamName, PutRecordsResult result){
        PutRecordsResultEntry resultEntry = result.getRecords().get(0);
        
        if (!StringUtils.isNullOrEmpty(resultEntry.getErrorCode()))
        {
            throw new RuntimeException(resultEntry.getErrorCode());
        }
        
        int partitionNum = getKafkaPartitionFromShardId(resultEntry.getPartitionId());
        
//        RecordMetadata recordMetadata =
//            new RecordMetadata(new TopicPartition(streamName, partitionNum), 0,
//                Long.parseLong(resultEntry.getSequenceNumber()));
        RecordMetadata recordMetadata =
            new RecordMetadata(new TopicPartition(streamName, partitionNum), 0,
                Long.parseLong(resultEntry.getSequenceNumber()), Record.NO_TIMESTAMP, -1, -1, -1);
        
        return recordMetadata;
    }
    
    private int getKafkaPartitionFromShardId(String shardId){
        int zeroIndex = shardId.indexOf("0");
        
        int partitionNum = Integer.parseInt(shardId.substring(zeroIndex == -1 ? 0 : zeroIndex));
        return partitionNum;
    }
    
    @Override
    public void flush()
    {
        producer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic)
    {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(topic);
        describeStreamRequest.setLimitPartitions(1);
        DescribeStreamResult describeStreamResult = disClient.describeStream(describeStreamRequest);
        for (int i = 0; i < describeStreamResult.getWritablePartitionCount(); i++)
        {
            partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), null, null));
        }
        return partitionInfos;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close()
    {
        this.close(10000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit unit)
    {
        this.producer.close();
    }
    
    private class RecordMetadataFuture implements Future<RecordMetadata>{
        
        private Future<PutRecordsResult> future;
        private String streamName;
        
        public RecordMetadataFuture(String streamName, Future<PutRecordsResult> putResultFuture)
        {
            this.future = putResultFuture;
            this.streamName = streamName;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled()
        {
            return future.isCancelled();
        }

        @Override
        public boolean isDone()
        {
            return future.isDone();
        }

        @Override
        public RecordMetadata get()
            throws InterruptedException, ExecutionException
        {
            PutRecordsResult result = future.get();
            if(result == null){
                return null;
            }
            return DISKafkaProducer.this.buildRecordMetadata(streamName, result);
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
        {
            PutRecordsResult result = future.get(timeout, unit);
            if(result == null){
                return null;
            }
            return DISKafkaProducer.this.buildRecordMetadata(streamName, result);
        }
        
    }
    
}
