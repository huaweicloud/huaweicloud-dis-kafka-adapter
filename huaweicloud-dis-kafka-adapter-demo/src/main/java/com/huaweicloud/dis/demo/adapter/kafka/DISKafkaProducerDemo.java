package com.huaweicloud.dis.demo.adapter.kafka;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.producer.*;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class DISKafkaProducerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISKafkaProducerDemo.class);

    public static void main(String[] args) {
        // YOU AK
        String ak = "YOU_AK";
        // YOU SK
        String sk = "YOU_SK";
        // YOU ProjectId
        String projectId = "YOU_PROJECT_ID";
        // YOU DIS Stream
        String streamName = "YOU_STREAM_NAME";
        // DIS region
        String region = "cn-north-1";

        Properties props = new Properties();
        props.setProperty(DISConfig.PROPERTY_AK, ak);
        props.setProperty(DISConfig.PROPERTY_SK, sk);
        props.setProperty(DISConfig.PROPERTY_PROJECT_ID, projectId);
        props.setProperty(DISConfig.PROPERTY_REGION_ID, region);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 默认情况下不需要设置endpoint，会自动使用域名访问；如需使用指定的endpoint，解除如下注释并设置endpoint即可
        // props.setProperty(DISConfig.PROPERTY_ENDPOINT, "https://dis.cn-north-1.myhuaweicloud.com");

        // 创建Producer，多个线程可共用同一个producer
        Producer<String, String> producer = new DISKafkaProducer<>(props);

        // 同步发送
        synchronousSendDemo(producer, streamName);

        // 异步发送
        asynchronousSendDemo(producer, streamName);

        // 关闭producer，防止资源泄露
        producer.close();
    }

    public static void synchronousSendDemo(Producer<String, String> producer, String streamName) {
        LOGGER.info("===== synchronous send =====");
        for (int i = 0; i < 5; i++) {
            // key设置为随机(或者设置为null)，数据会均匀分配到所有分区中
            String key = String.valueOf(ThreadLocalRandom.current().nextInt(1000000));
            String value = "Hello world[sync]. " + i;

            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(streamName, key, value));

            try {
                // 调用future.get会阻塞等待，直到发送完成
                RecordMetadata recordMetadata = future.get();
                // 发送成功
                LOGGER.info("Success to send [{}], Partition [{}], Offset [{}].",
                        value, recordMetadata.partition(), recordMetadata.offset());
            } catch (Exception e) {
                // 发送失败
                LOGGER.error("Failed to send [{}], Error [{}]", value, e.getMessage(), e);
            }
        }
    }

    public static void asynchronousSendDemo(Producer<String, String> producer, String streamName) {
        LOGGER.info("===== asynchronous send =====");
        int totalSendCount = 5;
        CountDownLatch countDownLatch = new CountDownLatch(totalSendCount);
        for (int i = 0; i < totalSendCount; i++) {
            // key设置为随机(或者设置为null)，数据会均匀分配到所有分区中
            String key = String.valueOf(ThreadLocalRandom.current().nextInt(1000000));
            String value = "Hello world[async]. " + i;

            try {
                // 使用回调方式发送，不会阻塞
                producer.send(new ProducerRecord<>(streamName, key, value), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        countDownLatch.countDown();
                        if (e == null) {
                            // 发送成功
                            LOGGER.info("Success to send [{}], Partition [{}], Offset [{}].",
                                    value, recordMetadata.partition(), recordMetadata.offset());
                        } else {
                            // 发送失败
                            LOGGER.error("Failed to send [{}], Error [{}]", value, e.getMessage(), e);
                        }
                    }
                });
            } catch (Exception e) {
                countDownLatch.countDown();
                LOGGER.error(e.getMessage(), e);
            }
        }

        try {
            // 等待所有发送完成
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}

