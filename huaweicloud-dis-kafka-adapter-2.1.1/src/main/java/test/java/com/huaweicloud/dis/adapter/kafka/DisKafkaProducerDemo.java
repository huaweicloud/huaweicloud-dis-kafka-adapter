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

package test.java.com.huaweicloud.dis.adapter.kafka;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.producer.DISKafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DisKafkaProducerDemo {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        /*DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");*/
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id", "zzz");
        disConfig.set("enable.auto.commit", "false");
        disConfig.set("ak","your ak");
        disConfig.set("sk","your sk");
        disConfig.set("projectId","your projectId");
        disConfig.set("region","your region");

        Producer<String, byte[]> disKafkaProducer = new DISKafkaProducer<>(disConfig);

        // 配置流名称，根据实际情况修改为与开通的DIS通道一致的“通道名称”
        String streamName = "dis-gzl0630";

        // 模拟生成待发送的数据
        ByteBuffer buffer = ByteBuffer.allocate(10);
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.put((byte) 'a');
        }

        Integer partitionId = new Integer("0");

        for (int i = 0; i < 100; i++) {
            partitionId = 0;
            String key = "" + i;
//            DisProducerRecord<String, byte[]> record = new DisProducerRecord<String, byte[]>(streamName, key, buffer.array());
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(streamName, partitionId, System.currentTimeMillis(), key, buffer.array());


            Future<RecordMetadata> future = disKafkaProducer.send(record);

            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata);
        }


        disKafkaProducer.close();

//        Producer<String, byte[]> kafkaProducer = new KafkaProducer<String, byte[]>(disConfig);
//        innerSend(kafkaProducer);
    }

}
