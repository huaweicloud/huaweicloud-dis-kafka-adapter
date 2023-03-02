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
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

@Configuration
public class BeanConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeanConfiguration.class);


    private static final String bootstrap = "128.10.47.24:22005";
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DISKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "128.10.47.24:22005");
        props.put("group.id","group");
        props.put("acks", "all");
        props.put("retries", "2");
        props.put("linger.ms", "1");
        props.put("buffer.memory", "33554432");
        props.put("compression.type","none");
        props.put("max.block.ms","6000");
        props.put("max.request.size","2097152");
        props.put("max.partition.fetch.bytes","2097152");
        props.put("max.in.flight.requests.per.connection","5000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        
        return props;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }
    

    @Bean
    public KafkaMessageListenerContainer<String, String> messageListenerContainer(){
        String topic = Test.topic;
        
        ContainerProperties properties = new ContainerProperties(topic);
        properties.setMessageListener(new MyMessageListener());
        return new KafkaMessageListenerContainer<String, String>(consumerFactory(), properties);
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DISKafkaConsumerFactory<String, String>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    
}
