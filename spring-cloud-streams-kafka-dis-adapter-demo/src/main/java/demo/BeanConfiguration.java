package demo;

import java.util.HashMap;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;

import com.huaweicloud.streamkafka.disadapter.DISKafkaConsumerFactory;
import com.huaweicloud.streamkafka.disadapter.DISKafkaProducerFactory;

@Configuration
public class BeanConfiguration
{
    
    @Bean
    public ConsumerFactory<?, ?> kafkaConsumerFactory() {
        return new DISKafkaConsumerFactory(new HashMap());
    }

    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory() {
        return new DISKafkaProducerFactory(new HashMap());
    }
    
}
