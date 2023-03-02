package com.cloud.dis.demo.adapter.kafka.thread;

import com.cloud.dis.adapter.kafka.clients.consumer.*;
import com.cloud.dis.adapter.kafka.common.TopicPartition;
import com.cloud.dis.demo.adapter.kafka.DISKafkaConsumerSubscribeDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DISKafkaConsumerSubscribeThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(DISKafkaConsumerSubscribeDemo.class);

    private Properties properties;

    private String streamName;

    public DISKafkaConsumerSubscribeThread(String name, String streamName, Properties properties) {
        super(name);
        this.properties = properties;
        this.streamName = streamName;
    }

    @Override
    public void run() {
        LOGGER.info("Running thread: {}.", this.getName());

        Consumer<String, String> consumer = new DISKafkaConsumer<>(properties);
        // 使用subscribe模式，指定需要消费的通道名即可
        consumer.subscribe(Collections.singleton(streamName));

        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            LOGGER.info("Value [{}], Partition [{}], Offset [{}], Key [{}]",
                                    record.value(), record.partition(), record.offset(), record.key());
                        }
                    }
                    // 数据处理完成之后异步提交当前offset(也可使用同步提交commitSync)
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if (e == null) {
                                LOGGER.debug("Success to commit offset [{}]", map);
                            } else {
                                LOGGER.error("Failed to commit offset [{}]", e.getMessage(), e);
                            }
                        }
                    });
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {

                }
            }
        }
    }

}
