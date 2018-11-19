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

package com.huaweicloud.dis.adapter.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.consumer.DISKafkaConsumer;

/**
 * Created by z00382129 on 2017/10/31.
 */
public class DisKafkaConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(DisKafkaConsumerDemo.class);
    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
     // lowLevelApiTest0();
    //    lowLevelApiTest1();
    //    highLevelApiTest1();
    //    highLevelApiTest2();
    //    highLevelApiTest3();
   //    highLevelApiTest4();
    //  highLevelApiTest5();
    //    highLevelApiTest6();
        highLevelApiTest7();
    }
    
    public static void lowLevelApiTest0()
    {
        DISConfig disConfig = DISConfig.buildDefaultConfig();
        try (Consumer<String, String> disConsumer = new DISKafkaConsumer<String, String>(disConfig)) {
            Map<String, List<PartitionInfo>> mp = disConsumer.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : mp.entrySet()) {
                log.info("topic: " + entry.getKey());
                for (PartitionInfo partitionInfo : entry.getValue()) {
                    log.info(partitionInfo.topic() + " " + partitionInfo.partition());
                }
            }
            List<TopicPartition> assign = new ArrayList<>();
            TopicPartition tp0 = new TopicPartition("zj-ttt", 0);
            TopicPartition tp1 = new TopicPartition("zj-ttt", 1);
            TopicPartition tp2 = new TopicPartition("zj-ttt", 2);
            assign.add(tp0);
            assign.add(tp1);
            assign.add(tp2);
            disConsumer.assign(assign);
            disConsumer.seekToEnd(assign);
            log.info("-----------------------------");
            log.info(tp0.toString() + " end position: " + disConsumer.position(tp0));
            disConsumer.seekToBeginning(assign);
            log.info(tp0.toString() + " beginning position " + disConsumer.position(tp0));
            long startTime = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> consumerRecords = disConsumer.poll(5000);
                if(!consumerRecords.records(tp0).isEmpty())
                {
                    for(ConsumerRecord record: consumerRecords.records(tp0))
                    {
                        System.out.println(record.toString());
                    }
                }
                if(System.currentTimeMillis() - startTime > 60*10000)
                {
                    break;
                }
            }
            disConsumer.close();
            try {
                disConsumer.poll(5000);
            }
            catch (IllegalStateException e)
            {
                log.info("close function is ok");
            }
        }
    }

    public static void lowLevelApiTest1()
    {
        DISConfig disConfig = DISConfig.buildDefaultConfig();
        try (Consumer<String, String> disConsumer = new DISKafkaConsumer<String, String>(disConfig)) {
            List<PartitionInfo> partitionInfos = disConsumer.partitionsFor("zj-ttt");
            for(PartitionInfo partitionInfo: partitionInfos)
            {
                log.info("partitionInfo " + partitionInfo.topic() + partitionInfo.partition());
            }
            List<TopicPartition> assign = new ArrayList<>();
            TopicPartition tp0 = new TopicPartition("zj-ttt", 0);
            assign.add(tp0);
            disConsumer.assign(assign);
            disConsumer.seekToEnd(assign);
            log.info("-----------------------------");
            long endPos = disConsumer.position(tp0);
            log.info(tp0.toString() + " end position: " + disConsumer.position(tp0));
            disConsumer.seekToBeginning(assign);
            long begining = disConsumer.position(tp0);
            log.info(tp0.toString() + " beginning position " + disConsumer.position(tp0));
            disConsumer.seek(tp0,(begining+endPos)/2);
            log.info("seek to " + (begining+endPos)/2);
            long startTime = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> consumerRecords = disConsumer.poll(5000);
                log.info(tp0.toString() + " size: " + consumerRecords.records(tp0).size());
                if(System.currentTimeMillis() - startTime > 60*1000)
                {
                    break;
                }
            }
            disConsumer.close();
        }
    }

    public static void highLevelApiTest1()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        /*
            enable.auto.commit 与 auto.commit.interval.ms一起使用，auto.commit.interval.ms表示按周期提交checkpoint的时间，
            客户端会尽力按照这个时间提交checkpoint， 需要在数据库检查checkpoint的提交
         */
        disConfig.set("enable.auto.commit","true");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Pattern.compile("^zj-t.*"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                log.info("onPartitionsRevoked ");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                log.info("onPartitionsAssigned ");
            }
        });
        TopicPartition tp0 = new TopicPartition("zj-ttt",0);
        TopicPartition tp1 = new TopicPartition("zj-ttt",1);
        TopicPartition tp2 = new TopicPartition("zj-ttt",2);
        long startTime = System.currentTimeMillis();
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            log.info(tp0.toString() + " size: " + consumerRecords.records(tp1).size());
            log.info(tp1.toString() + " size: " + consumerRecords.records(tp2).size());
            log.info(tp2.toString() + " size: " + consumerRecords.records(tp0).size());
            if(System.currentTimeMillis() - startTime < 30000)
            {
                log.info("assignment " + disConsumer.assignment());
                log.info("subscription " + disConsumer.subscription());
            }
            if(System.currentTimeMillis() -  startTime > 32*1000)
            {
                break;
            }
        }
        disConsumer.close();
    }

    public static void highLevelApiTest2()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        //采用手动提交checkpoint
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Collections.singleton("zj-ttt"));
        TopicPartition tp0 = new TopicPartition("zj-ttt",0);
        TopicPartition tp1 = new TopicPartition("zj-ttt",1);
        TopicPartition tp2 = new TopicPartition("zj-ttt",2);
        long startTime = System.currentTimeMillis();
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            log.info(tp0.toString() + " size: " + consumerRecords.records(tp1).size());
            log.info(tp1.toString() + " size: " + consumerRecords.records(tp2).size());
            log.info(tp2.toString() + " size: " + consumerRecords.records(tp0).size());
            if(System.currentTimeMillis() -  startTime < 60*1000 && !disConsumer.assignment().isEmpty())
            {
                log.info("disConsumer.commitSync() ");
                log.info(tp0.toString() + " current position " + disConsumer.position(tp0));
                //同步提分配给他的partition的checkpoint
                disConsumer.commitSync();
                log.info(tp0.toString() + " get checkpoint " + disConsumer.committed(tp0).toString());
            }
            if(System.currentTimeMillis() -  startTime >= 60*1000 && System.currentTimeMillis() -  startTime < 120*1000)
            {
                log.info("disConsumer.commitSync(offsetAndMetadataMap) ");
                log.info(tp0.toString() + " current position " + disConsumer.position(tp0));
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
                offsetAndMetadataMap.put(tp0,new OffsetAndMetadata(disConsumer.position(tp0),"metadata"));
                disConsumer.commitSync(offsetAndMetadataMap);
                //检查metadata 是否正确
                log.info(tp0.toString() + " get checkpoint " + disConsumer.committed(tp0).toString());
            }
            if(System.currentTimeMillis() -  startTime >= 120*1000 && System.currentTimeMillis() -  startTime < 180*1000)
            {
                log.info("disConsumer.commitAsync() ");
                log.info(tp0.toString() + " current position " + disConsumer.position(tp0));
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
                //异步提分配给他的partition的checkpoint
                disConsumer.commitAsync();
                log.info(tp0.toString() + " get checkpoint " + disConsumer.committed(tp0).toString());
            }
            if(System.currentTimeMillis() -  startTime >= 180*1000 && System.currentTimeMillis() -  startTime < 240*1000)
            {
                log.info("disConsumer.commitAsync(new OffsetCommitCallback()) ");
                log.info(tp0.toString() + " current position " + disConsumer.position(tp0));
                //异步提分配给他的partition的checkpoint
                disConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if(e == null)
                        {
                            log.info("commit checkpoint successful");
                        }
                        else
                        {
                            log.error(e.getMessage(),e);
                        }
                    }
                });
                log.info(tp0.toString() + " get checkpoint " + disConsumer.committed(tp0).toString());
            }
            if(System.currentTimeMillis() -  startTime >= 240*1000 && System.currentTimeMillis() -  startTime < 320*1000)
            {
                log.info("disConsumer.commitAsync(offsetAndMetadataMap, new OffsetCommitCallback()) ");
                log.info(tp0.toString() + " current position " + disConsumer.position(tp0));
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
                offsetAndMetadataMap.put(tp0,new OffsetAndMetadata(disConsumer.position(tp0),"metadata2"));
                //异步提分配给他的partition的checkpoint
                disConsumer.commitAsync(offsetAndMetadataMap,new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if(e == null)
                        {
                            log.info("commitAsync successful");
                        }
                        else
                        {
                            log.error(e.getMessage(),e);
                        }
                    }
                });
                log.info(tp0.toString() + " get checkpoint " + disConsumer.committed(tp0).toString());
            }
        }
    }

    public static void highLevelApiTest3()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Collections.singleton("zj-ttt"));
        TopicPartition tp0 = new TopicPartition("zj-ttt",0);
        TopicPartition tp1 = new TopicPartition("zj-ttt",1);
        TopicPartition tp2 = new TopicPartition("zj-ttt",2);
        long startTime = System.currentTimeMillis();
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            log.info(tp0.toString() + " size: " + consumerRecords.records(tp1).size());
            log.info(tp1.toString() + " size: " + consumerRecords.records(tp2).size());
            log.info(tp2.toString() + " size: " + consumerRecords.records(tp0).size());
            if(System.currentTimeMillis() - startTime < 30000 && !disConsumer.assignment().isEmpty())
            {
                // pause tp0
                //观察 tp0 是否还有数据读取
                log.info("paused0 " + disConsumer.paused());
                disConsumer.pause(Collections.singleton(tp0));
                log.info("paused1 " + disConsumer.paused());
            }
            if(System.currentTimeMillis() - startTime > 600000)
            {
                // resume tp0
                log.info("paused2 " + disConsumer.paused());
                disConsumer.resume(Collections.singleton(tp0));
                log.info("paused3 " + disConsumer.paused());
            }
            if(System.currentTimeMillis() - startTime > 900000)
            {
                break;
            }
        }
    }

    public static void highLevelApiTest4()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Collections.singleton("zj-f"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //在rebalance前调用
                log.info("onPartitionsRevoked ");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //在rebalance成功后调用
                log.info("size " + collection.size());
                log.info("onPartitionsAssigned ");
            }
        });
        TopicPartition tp0 = new TopicPartition("zj-f",0);
        TopicPartition tp1 = new TopicPartition("zj-f",1);
        TopicPartition tp2 = new TopicPartition("zj-ttt",2);
        long startTime = System.currentTimeMillis();
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            log.info("assigment " + disConsumer.assignment());
            log.info(tp0.toString() + " size: " + consumerRecords.records(tp0).size());
            if(System.currentTimeMillis() - startTime > 900000)
            {
                break;
            }
        }
        //是否触发group的rebalance
        disConsumer.unsubscribe();
        log.info("assignment " + disConsumer.assignment());
        log.info("subscription " + disConsumer.subscription());
        disConsumer.close();
    }

    public static void highLevelApiTest5()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Arrays.asList("zj-f","zj-t"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //在rebalance前调用
                log.info("onPartitionsRevoked ");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //在rebalance成功后调用
                log.info("onPartitionsAssigned ");
            }
        });
        TopicPartition tp0 = new TopicPartition("zj-f",0);
        TopicPartition tp1 = new TopicPartition("zj-f",1);
        TopicPartition tp2 = new TopicPartition("zj-ttt",2);
        long startTime = System.currentTimeMillis();
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            log.info("assigment " + disConsumer.assignment());
            if(System.currentTimeMillis() - startTime > 9000000)
            {
                break;
            }
        }
    }

    public static void highLevelApiTest6()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        disConsumer.subscribe(Pattern.compile("zj-.*"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //在rebalance前调用
                System.out.println("onPartitionsRevoked ");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //在rebalance成功后调用
                System.out.println("onPartitionsAssigned ");
            }
        });
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            System.out.println("assigment " + disConsumer.assignment());

        }
    }

    public static void highLevelApiTest7()
    {
        DISConfig disConfig = new DISConfig();
        disConfig.set("IS_DEFAULT_TRUSTED_JKS_ENABLED", "false");
        disConfig.set("group.id","app");
        disConfig.set("enable.auto.commit","false");
        log.info("groupId " + disConfig.getGroupId());
        Consumer<String, String > disConsumer = new DISKafkaConsumer<String, String>(disConfig);
        List<String> sub = new ArrayList<>();
        sub.add("zj-ttt");
        sub.add("zj-pppp");
        disConsumer.subscribe(sub, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //在rebalance前调用
                System.out.println("onPartitionsRevoked ");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //在rebalance成功后调用
                System.out.println("onPartitionsAssigned ");
            }
        });
        while (true)
        {
            ConsumerRecords<String,String > consumerRecords = disConsumer.poll(5000);
            System.out.println("assigment " + disConsumer.assignment());

        }
    }
    
}
