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
package com.huawei.springkafka.test;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class Test
{
    public static String topic = "t1";
    
    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        ApplicationContext context = new ClassPathXmlApplicationContext("/applicationContext.xml");
        
        Test p = context.getBean(Test.class);

        p.testKafka();
    }
    
    private void testKafka() throws InterruptedException, ExecutionException
    {
        messageListenerContainer.start();
        
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, 0, null, "testtest");

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("haha");
            }

            @Override
            public void onFailure(Throwable ex) {
                ex.printStackTrace();
            }

        });
        future.get();
        System.out.println("over..");
    }    
    
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    KafkaMessageListenerContainer<String, String> messageListenerContainer;
    
}

class MyMessageListener implements MessageListener<String, String> {
    @Override
    public void onMessage(ConsumerRecord<String, String> data) {

        System.out.println(data.topic() + ":" + data.value());
    }
}
