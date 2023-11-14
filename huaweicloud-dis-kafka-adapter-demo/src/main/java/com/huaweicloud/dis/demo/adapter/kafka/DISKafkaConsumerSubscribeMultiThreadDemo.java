package com.huaweicloud.dis.demo.adapter.kafka;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.*;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer;
import com.huaweicloud.dis.demo.adapter.kafka.thread.DISKafkaConsumerSubscribeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DISKafkaConsumerSubscribeMultiThreadDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISKafkaConsumerSubscribeMultiThreadDemo.class);

    private static final String path = "dis.properties";
    public static void main(String[] args) {
        // 从配置文件中获取基础信息
        Properties props = new Properties();
        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            props.load(fileInputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // LATEST: 从最新的数据之后开始消费；EARLIEST: 从最早的数据开始消费
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name());
        String streamName = props.getProperty("streamName");

        // 默认情况下不需要设置endpoint，会自动使用域名访问；如需使用指定的endpoint，解除如下注释并设置endpoint即可
         props.setProperty(DISConfig.PROPERTY_ENDPOINT, "https://dis.cn-north-1.myhuaweicloud.com");

        ExecutorService executorServicePool = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 5; i++) {
            executorServicePool.submit(new DISKafkaConsumerSubscribeThread("consumer" + i, streamName, props));
        }

    }
}
