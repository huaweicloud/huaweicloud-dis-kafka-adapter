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

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.producer.DISKafkaProducer;
import com.huaweicloud.dis.adapter.kafka.clients.producer.Producer;
import com.huaweicloud.dis.adapter.kafka.clients.producer.ProducerRecord;
import com.huaweicloud.dis.adapter.kafka.clients.producer.RecordMetadata;


public class ProducerDemo {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        Producer<String, byte[]> disProducer = null;
        
        try
        {
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
            
            disProducer = new DISKafkaProducer<>(disConfig);
            
            // 配置流名称，根据实际情况修改为与开通的DIS通道一致的“通道名称”
            String streamName = "stz_test";
            
            // 模拟生成待发送的数据
            ByteBuffer buffer = ByteBuffer.allocate(10);
            for (int i = 0; i < buffer.capacity(); i++) {
                buffer.put((byte) 'a');
            }
            
            Integer partitionId = new Integer("0");
            
            for (int i = 0; i < 100; i++) {
                partitionId = 0;
                String key = "" + i;
                ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(streamName, partitionId,
                    System.currentTimeMillis(), key, buffer.array());
                
                Future<RecordMetadata> future = disProducer.send(record);

                RecordMetadata recordMetadata = future.get();
                System.out.println(recordMetadata);
            }
            
            Thread.sleep(5 * 1000);
        }
        finally
        {
            if (disProducer != null)
            {
                disProducer.close();
            }
        }
        
    }

}
