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

import com.huaweicloud.dis.adapter.kafka.consumer.DISKafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.Ignore;
import org.junit.Test;

import com.huaweicloud.dis.DISConfig;

/**
 * Created by z00382129 on 2017/11/24.
 */
@Ignore
public class consumerKVTest {

    @Test
    public void KVTest0()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        disConfig.set("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        DISKafkaConsumer<String,String> disKafkaConsumer = new DISKafkaConsumer<String, String>(disConfig);
    }

    @Test
    public void KVTest1()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("key.deserializer","");
        disConfig.set("value.deserializer","");
        ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
        DISKafkaConsumer<byte[],byte[]> disKafkaConsumer = new DISKafkaConsumer<byte[],byte[]>(disConfig,byteArrayDeserializer,byteArrayDeserializer);
    }

    @Test
    public void KVTest2()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("key.deserializer","");
        disConfig.set("value.deserializer","");
        ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
        LongDeserializer longDeserializer = new LongDeserializer();
        DISKafkaConsumer<Long,byte[]> disKafkaConsumer = new DISKafkaConsumer<Long,byte[]>(disConfig,longDeserializer,byteArrayDeserializer);
    }

    @Test
    public void KVTest3()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer");
        disConfig.set("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        DISKafkaConsumer<String,String> disKafkaConsumer = new DISKafkaConsumer<String, String>(disConfig);
    }
    @Test
    public void KVTest4()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        disConfig.set("value.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        DISKafkaConsumer<String,String> disKafkaConsumer = new DISKafkaConsumer<String, String>(disConfig);
    }


}
