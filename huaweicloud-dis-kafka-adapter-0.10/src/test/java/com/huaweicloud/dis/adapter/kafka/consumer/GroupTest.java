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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.consumer.DISKafkaConsumer;

/**
 * Created by z00382129 on 2017/11/27.
 */
@Ignore
public class GroupTest {
    private static final Logger log = LoggerFactory.getLogger(GroupTest.class);
    public boolean valid(List<ConsumerRecord> records)
    {
        if(records == null || records.isEmpty())
        {
            log.info("records in empty");
            return true;
        }
        log.info("beginning {}", records.get(0).offset());
        log.info("end {}", records.get(records.size()-1).offset());
        long lastOffset = records.get(0).offset();
        for(ConsumerRecord record: records)
        {
            if(record.offset()==lastOffset)
            {
                lastOffset++;
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    class Consumer1 implements Runnable{
        private final Logger log = LoggerFactory.getLogger(Consumer1.class);
        private List<String > topics;
        private String clientId;
        private DISKafkaConsumer<String,String> consumer;
        private long timeout;
        private CountDownLatch latch;
        public Consumer1(String clientId,List<String > topics, long timeout,CountDownLatch latch)
        {
            this.timeout = timeout;
            this.latch = latch;
            for(String topic:topics)
            {
                log.info("subscribe {}",topic);
            }
            this.clientId = clientId;
            this.topics = topics;
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.set("client.id",clientId);
            consumer = new DISKafkaConsumer<String, String>(disConfig);
            consumer.subscribe(topics);
        }

        @Override
        public void run() {
            try {
                doWork();
            }finally {
                latch.countDown();
            }
        }
        private void doWork()
        {
            log.info(clientId + " starting --------------------------");
            long startTime = System.currentTimeMillis();
            while (true)
            {
                if(System.currentTimeMillis() - startTime > timeout)
                {
                    break;
                }
                ConsumerRecords consumerRecords = consumer.poll(10000);
                log.info("assignment -------" );
                for(TopicPartition partition: consumer.assignment())
                {
                    log.info("get assignment " + partition);
                    if(!valid(consumerRecords.records(partition)))
                    {
                        log.error("records error for partition {}", partition);
                    }
                }
            }
        }

    }

    class Consumer2 implements Runnable{
        private final Logger log = LoggerFactory.getLogger(Consumer1.class);
        private List<String > topics;
        private String clientId;
        private DISKafkaConsumer<String,String> consumer;
        private long timeout;
        private CountDownLatch latch;
        public Consumer2(String clientId, List<String > topics, ConsumerRebalanceListener callback, long timeout,CountDownLatch latch)
        {
            this.timeout = timeout;
            this.latch = latch;
            for(String topic:topics)
            {
                log.info("subscribe {}",topic);
            }
            this.clientId = clientId;
            this.topics = topics;
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.set("client.id",clientId);
            consumer = new DISKafkaConsumer<String, String>(disConfig);
            consumer.subscribe(topics,callback);
        }

        @Override
        public void run() {
            try {
                doWork();
            }finally {
                latch.countDown();
            }
        }
        private void doWork()
        {
            log.info(clientId + " starting --------------------------");
            long startTime = System.currentTimeMillis();
            while (true)
            {
                if(System.currentTimeMillis() - startTime > timeout)
                {
                    break;
                }
                ConsumerRecords consumerRecords = consumer.poll(10000);
                log.info("assignment -------" );
                for(TopicPartition partition: consumer.assignment())
                {
                    log.info("get assignment " + partition);
                    if(!valid(consumerRecords.records(partition)))
                    {
                        log.error("records error for partition {}", partition);
                    }
                }
            }
        }
    }

    class Consumer3 implements Runnable{
        private final Logger log = LoggerFactory.getLogger(Consumer1.class);
        private String clientId;
        private DISKafkaConsumer<String,String> consumer;
        private long timeout;
        private CountDownLatch latch;
        public Consumer3(String clientId, String pattern, ConsumerRebalanceListener callback, long timeout,CountDownLatch latch)
        {
            this.latch = latch;
            this.timeout = timeout;
            this.clientId = clientId;
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.set("client.id",clientId);
            consumer = new DISKafkaConsumer<String, String>(disConfig);
            consumer.subscribe(Pattern.compile(pattern),callback);
        }

        @Override
        public void run() {
            try {
                doWork();
            }finally {
                latch.countDown();
            }
        }
        private void doWork()
        {
            log.info(clientId + " starting --------------------------");
            long startTime = System.currentTimeMillis();
            while (true)
            {
                if(System.currentTimeMillis() - startTime > timeout)
                {
                    break;
                }
                ConsumerRecords consumerRecords = consumer.poll(10000);
                log.info("subscription -----------");
                for(String topic:consumer.subscription())
                {
                    log.info("subscribe {}",topic);
                }
                log.info("assignment -------" );
                for(TopicPartition partition: consumer.assignment())
                {
                    log.info("get assignment " + partition);
                    if(!valid(consumerRecords.records(partition)))
                    {
                        log.error("records error for partition {}", partition);
                    }
                }
            }
        }
    }

    @Test
    public void groupTest0()
    {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(new Consumer1("client1", Arrays.asList("zj-ttt"),60*4*1000,countDownLatch));
        thread.setName("client0");
        thread.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest1()
    {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(new Consumer1("client1", Arrays.asList("zj-ttt","zj-f"),60*4*1000,countDownLatch));
        thread.setName("client1");
        thread.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest2()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t2 = new Thread(new Consumer1("client2", Arrays.asList("zj-ttt"),60*4*1000,countDownLatch));
        t2.setName("client2");
        t2.start();
        Thread t3 = new Thread(new Consumer1("client3", Arrays.asList("zj-ttt"),60*4*1000,countDownLatch));
        t3.setName("client3");
        t3.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest3()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t4 = new Thread(new Consumer1("client4", Arrays.asList("zj-ttt","zj-f"),60*4*1000,countDownLatch));
        t4.setName("client4");
        t4.start();
        Thread t5 = new Thread(new Consumer1("client5", Arrays.asList("zj-ttt","zj-f"),60*4*1000,countDownLatch));
        t5.setName("client5");
        t5.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    public class RebalanceCallback implements ConsumerRebalanceListener
    {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
             log.info("onPartitionsRevoked =======");
             for(TopicPartition partition: collection)
             {
                 log.info(partition.toString());
             }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            log.info("onPartitionsAssigned =======");
            for(TopicPartition partition: collection)
            {
                log.info(partition.toString());
            }
        }
    }

    @Test
    public void groupTest4()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t4 = new Thread(new Consumer2("client6", Arrays.asList("zj-ttt","zj-f"),new RebalanceCallback(),60*4*1000,countDownLatch));
        t4.setName("client6");
        t4.start();
        Thread t5 = new Thread(new Consumer2("client7", Arrays.asList("zj-ttt","zj-f"),new RebalanceCallback(),60*4*1000,countDownLatch));
        t5.setName("client7");
        t5.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest5()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t4 = new Thread(new Consumer2("client8", Arrays.asList("zj-ttt","zj-f"),new RebalanceCallback(),60*5*1000,countDownLatch));
        t4.setName("client8");
        t4.start();
        try {
            Thread.sleep(40*1000);
        }
        catch (InterruptedException e)
        {

        }
        Thread t5 = new Thread(new Consumer2("client9", Arrays.asList("zj-ttt","zj-f"),new RebalanceCallback(),60*1000,countDownLatch));
        t5.setName("client9");
        t5.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest6()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t4 = new Thread(new Consumer3("client10", "^zj-.*$",new RebalanceCallback(),60*4*1000,countDownLatch));
        t4.setName("client10");
        t4.start();
        Thread t5 = new Thread(new Consumer3("client11", "^zj-t.*$",new RebalanceCallback(),60*4*1000,countDownLatch));
        t5.setName("client11");
        t5.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    @Test
    public void groupTest7()
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread t4 = new Thread(new Consumer3("client12", "^zj-.*$",new RebalanceCallback(),60*5*1000,countDownLatch));
        t4.setName("client12");
        t4.start();
        try {
            Thread.sleep(40*1000);
        }
        catch (InterruptedException e)
        {

        }
        Thread t5 = new Thread(new Consumer3("client13", "^zj-t.*$",new RebalanceCallback(),60*1000,countDownLatch));
        t5.setName("client13");
        t5.start();
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

}
