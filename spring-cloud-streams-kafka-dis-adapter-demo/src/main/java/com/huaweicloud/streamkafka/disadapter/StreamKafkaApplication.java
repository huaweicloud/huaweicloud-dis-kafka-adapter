package com.huaweicloud.streamkafka.disadapter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import demo.Greetings;
import demo.GreetingsService;

@SpringBootApplication
public class StreamKafkaApplication
{
    
    public static void main(String[] args) throws InterruptedException
    {
        ConfigurableApplicationContext context = SpringApplication.run(StreamKafkaApplication.class, args);
        GreetingsService service = context.getBean(GreetingsService.class);
        
        while(true){
            Greetings greetings = new Greetings();
            greetings.setMessage("hahahhha");
            greetings.setTimestamp(System.currentTimeMillis());
            
            service.sendGreeting(greetings);
            Thread.sleep(5000L);
        }
        
    }
}
