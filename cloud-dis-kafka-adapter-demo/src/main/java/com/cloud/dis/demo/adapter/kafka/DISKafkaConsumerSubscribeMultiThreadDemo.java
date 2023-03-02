package com.cloud.dis.demo.adapter.kafka;

import com.cloud.dis.DISConfig;
import com.cloud.dis.adapter.kafka.clients.consumer.*;
import com.cloud.dis.adapter.kafka.common.serialization.StringDeserializer;
import com.cloud.dis.demo.adapter.kafka.thread.DISKafkaConsumerSubscribeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DISKafkaConsumerSubscribeMultiThreadDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISKafkaConsumerSubscribeMultiThreadDemo.class);

    public static void main(String[] args) {

        // YOU AK
        String ak = "YOU_AK";
        // YOU SK
        String sk = "YOU_SK";
        // YOU ProjectId
        String projectId = "YOU_PROJECT_ID";
        // YOU DIS Stream
        String streamName = "YOU_STREAM_NAME";
        // 消费组ID，用于记录offset和进行group rebalance
        String groupId = "YOU_GROUP_ID";
        // DIS region
        String region = "cn-north-1";

        Properties props = new Properties();
        props.setProperty(DISConfig.PROPERTY_AK, ak);
        props.setProperty(DISConfig.PROPERTY_SK, sk);
        props.setProperty(DISConfig.PROPERTY_PROJECT_ID, projectId);
        props.setProperty(DISConfig.PROPERTY_REGION_ID, region);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // LATEST: 从最新的数据之后开始消费；EARLIEST: 从最早的数据开始消费
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name());

        // 默认情况下不需要设置endpoint，会自动使用域名访问；如需使用指定的endpoint，解除如下注释并设置endpoint即可
         props.setProperty(DISConfig.PROPERTY_ENDPOINT, "https://dis-cn-north-1.mycloud.com");

        ExecutorService executorServicePool = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 5; i++) {
            executorServicePool.submit(new DISKafkaConsumerSubscribeThread("consumer" + i, streamName, props));
        }

    }
}
