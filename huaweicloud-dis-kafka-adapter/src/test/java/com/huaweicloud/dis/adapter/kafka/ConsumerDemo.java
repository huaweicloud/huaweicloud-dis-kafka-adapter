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

package com.huaweicloud.dis.adapter.kafka;

import java.util.Arrays;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.Consumer;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRecord;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRecords;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;


public class ConsumerDemo {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) throws InterruptedException {
        
        Consumer<String, String> disConsumer = null;
        // 配置流名称，根据实际情况修改为与开通的DIS通道一致的“通道名称”
        String streamName = "stz_test";
        
        try
        {
            
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
            disConfig.set("group.id", "app");
            disConfig.set("enable.auto.commit", "true");
            
            disConsumer = new DISKafkaConsumer<String, String>(disConfig);
            disConsumer.subscribe(Arrays.asList(new String[]{streamName}), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    LOGGER.info("onPartitionsRevoked ");
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    LOGGER.info("onPartitionsAssigned ");
                }
            });
            
            while (true)
            {
                ConsumerRecords<String, String> consumerRecords = disConsumer.poll(5000);
                LOGGER.info("Records size: {}.", consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords)
                {
                    String recordKey = record.key();
                    String recordValue = record.value();
                    
                    LOGGER.info("Record Key: {}, Record Value: {}.", recordKey, recordValue);
                }
                
                Thread.sleep(5 * 1000);
            }
        }
        finally
        {
            if (disConsumer != null)
            {
                disConsumer.close();
            }
        }
        
    }

}
