package com.huaweicloud.dis.demo.adapter.kafka;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.consumer.*;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class DISKafkaConsumerSubscribePatternDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISKafkaConsumerSubscribePatternDemo.class);

    public static void main(String[] args) {
        // YOU AK
        String ak = "YOU_AK";
        // YOU SK
        String sk = "YOU_SK";
        // YOU ProjectId
        String projectId = "YOU_PROJECT_ID";
        // 待消费的通道名通配符(stream.* 表示会消费 stream1, stream2, stream_123等等)
        String streamNamePattern = "stream.*";
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
        // props.setProperty(DISConfig.PROPERTY_ENDPOINT, "https://dis-cn-north-1.myhuaweicloud.com");

        Consumer<String, String> consumer = new DISKafkaConsumer<>(props);
        // 使用subscribePattern模式，指定通配符即可
        consumer.subscribe(Pattern.compile(streamNamePattern), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // rebalance前的消费分区
                LOGGER.info("onPartitionsRevoked [{}]", collection);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // rebalance后的消费分区
                LOGGER.info("onPartitionsAssigned [{}]", collection);
            }
        });

        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(5000);

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
                LOGGER.info(e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {

                }
            }
        }
    }
}

