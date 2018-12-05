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

package com.huaweicloud.dis.adapter.kafka.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.consumer.DISKafkaConsumer;
import com.huaweicloud.dis.exception.DISClientException;

/**
 * Created by z00382129 on 2017/11/22.
 */
@Ignore
public class commonConsumerTest {

    private Consumer<String, String> disConsumer;

    @Before
    public void setup() {
        DISConfig disConfig = DISConfig.buildDefaultConfig();
        disConsumer = new DISKafkaConsumer<String, String>(disConfig);
    }


    @Test
    public void listTopicTest() {
        //这里仅打印获取到的topic和partition信息，是否正确需要验证
        Map<String, List<PartitionInfo>> mp = disConsumer.listTopics();
        for (Map.Entry<String, List<PartitionInfo>> entry : mp.entrySet()) {
            System.out.println("--------------topic: " + entry.getKey());
            for (PartitionInfo partitionInfo : entry.getValue()) {
                System.out.println(partitionInfo.topic() + " " + partitionInfo.partition());
            }
        }
    }

    @Test
    public void partitionForTest() {
        String topic = "zj-ttt";
        List<PartitionInfo> partitionInfos = disConsumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
            System.out.println("partitionInfo " + partitionInfo.topic() + " " + partitionInfo.partition());
        }
    }

    @Test
    public void commitTest0() {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id", "");
        String topic = "zj-ttt";
        DISKafkaConsumer consumer = new DISKafkaConsumer<String, String>(disConfig);
        try {
            consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, 0), new OffsetAndMetadata(100, "testMeta")));
        } catch (IllegalStateException e) {
            System.out.println("error msg " + e.getMessage());
            Assert.assertTrue(e.getMessage().equals("groupId not defined, checkpoint not commit"));
        }
    }

    @Test
    public void commitTest1() {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        //group必须存在
        disConfig.set("group.id", "testGroup");
        String topic = "testTopic";
        DISKafkaConsumer consumer = new DISKafkaConsumer<String, String>(disConfig);
        try {
            consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, 0), new OffsetAndMetadata(100, "testMeta")));
        } catch (IllegalStateException e) {
            System.out.println("error msg " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains(" is not assigned!"));
        }
    }

    @Test
    public void commitTest2() {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        //group必须存在
        disConfig.set("group.id", "app");
        String topic = "zj-ttt";
        int partition = 0;
        long checkpoint = 100;
        DISKafkaConsumer consumer = new DISKafkaConsumer<String, String>(disConfig);
        consumer.assign(Collections.singleton(new TopicPartition(topic, partition)));
        try {
            consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(checkpoint, "testMeta")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        OffsetAndMetadata offsetAndMetadata = consumer.committed(new TopicPartition(topic, partition));
        Assert.assertTrue(checkpoint == offsetAndMetadata.offset());
        Assert.assertTrue(offsetAndMetadata.metadata().equals("testMeta"));
    }

    @Test
    public void commitTest3() {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        //group必须存在
        disConfig.set("group.id", "testGroup");
        String topic = "testTopic";
        int partition = 0;
        long checkpoint = 100;
        DISKafkaConsumer consumer = new DISKafkaConsumer<String, String>(disConfig);
        consumer.subscribe(Collections.singleton(topic));
        try {
            consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, partition), new OffsetAndMetadata(checkpoint, "testMeta")));
        } catch (IllegalStateException e) {
            System.out.println("error msg " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains(" is not assigned!"));
        }
    }

    @Test
    public void closeTest0() {
        disConsumer.close();
    }

    @Test
    public void closeTest1() {
        disConsumer.close();
        try {
            disConsumer.assignment();
        } catch (IllegalStateException e) {

        }
    }

    @Test
    public void assignTest0() {
        String topic = "zj-ttt";
        int partition = 0;
        disConsumer.assign(Collections.singleton(new TopicPartition(topic,partition)));
        Set<TopicPartition> partitions = disConsumer.assignment();
        Assert.assertTrue(partitions.contains(new TopicPartition(topic,partition)));
    }

    @Test
    public void assignTest1(){
        String topic = "zj-ttt";
        disConsumer.subscribe(Collections.singleton(topic));
        Set<TopicPartition> partitions = disConsumer.assignment();
        Assert.assertTrue(partitions.isEmpty());
    }

    @Test
    public void assignTest2()
    {
        String topic = "zj-ttt";
        int partition = 0;
        disConsumer.assign(Collections.singleton(new TopicPartition(topic,partition)));
        disConsumer.poll(10000);
        Set<TopicPartition> partitions = disConsumer.assignment();
        Assert.assertTrue(partitions.contains(new TopicPartition(topic,partition)));
    }

    @Test
    public void assignTest3(){
        String topic = "zj-ttt";
        disConsumer.subscribe(Collections.singleton(topic));
        Set<TopicPartition> partitions = disConsumer.assignment();
        try {
            disConsumer.poll(10000);
        }catch (DISClientException e)
        {
            e.printStackTrace();
            String ex = e.getMessage();
            Assert.assertTrue(e.getMessage().contains("URL not found."));
        }
    }

    @Test
    public void assignTest4(){
        String topic = "zj-ttt";
        disConsumer.subscribe(Collections.singleton(topic));
        Set<TopicPartition> partitions = disConsumer.assignment();
        try {
            disConsumer.poll(10000);
        }catch (DISClientException e)
        {
            e.printStackTrace();
            String ex = e.getMessage();
            Assert.assertTrue(e.getMessage().contains("URL not found."));
        }
    }

}
