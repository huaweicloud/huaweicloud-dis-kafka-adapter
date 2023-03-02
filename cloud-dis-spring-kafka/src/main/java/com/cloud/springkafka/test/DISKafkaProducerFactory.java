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
package com.cloud.springkafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.kafka.core.ProducerFactory;

import com.cloud.dis.DISConfig;
import com.cloud.dis.adapter.kafka.producer.DISKafkaProducer;

public class DISKafkaProducerFactory <K, V> implements ProducerFactory<K, V>, Lifecycle, DisposableBean {

    private static final int DEFAULT_PHYSICAL_CLOSE_TIMEOUT = 30;

    private static final Log logger = LogFactory.getLog(DISKafkaProducerFactory.class);

    private final Map<String, Object> configs;

    private volatile CloseSafeProducer<K, V> producer;

    private Serializer<K> keySerializer;

    private Serializer<V> valueSerializer;

    private int physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

    private volatile boolean running;

    public DISKafkaProducerFactory(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public DISKafkaProducerFactory(Map<String, Object> configs, Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        this.configs = new HashMap<>(configs);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public void setKeySerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    public void setValueSerializer(Serializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    /**
     * The time to wait when physically closing the producer (when {@link #stop()} or {@link #destroy()} is invoked.
     * Specified in seconds; default {@value #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
     * @param physicalCloseTimeout the timeout in seconds.
     * @since 1.0.7
     */
    public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
        this.physicalCloseTimeout = physicalCloseTimeout;
    }

    @Override
    public void destroy() throws Exception { //NOSONAR
        CloseSafeProducer<K, V> producer = this.producer;
        this.producer = null;
        if (producer != null) {
            producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
        }
    }


    @Override
    public void start() {
        this.running = true;
    }


    @Override
    public void stop() {
        try {
            destroy();
        }
        catch (Exception e) {
            logger.error("Exception while stopping producer", e);
        }
    }


    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public Producer<K, V> createProducer() {
        if (this.producer == null) {
            synchronized (this) {
                if (this.producer == null) {
                    this.producer = new CloseSafeProducer<K, V>(createKafkaProducer());
                }
            }
        }
        return this.producer;
    }

    protected Producer<K, V> createKafkaProducer() {
        DISConfig config = new DISConfig();
        config.setEndpoint("https://10.40.48.158:21250");
        config.setProperty("manager.endpoint", "https://10.40.48.158:21250");
        config.setAK("CWQCEIGWRDFYNYTHXVBD");
        config.setSK("hWGwgAsWdqinap7TlgYiqxsGGtP2rADP9UXWPNlV");
        config.setProjectId("2fdbbc096fb0420489021acc8b802105");
        config.setRegion("southchina");
        
        return new DISKafkaProducer(config);
    }

    private static class CloseSafeProducer<K, V> implements Producer<K, V> {

        private final Producer<K, V> delegate;

        CloseSafeProducer(Producer<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return this.delegate.send(record);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return this.delegate.send(record, callback);
        }

        @Override
        public void flush() {
            this.delegate.flush();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return this.delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return this.delegate.metrics();
        }

        @Override
        public void close() {
        }

        @Override
        public void close(long timeout, TimeUnit unit) {
        }

    }

}
