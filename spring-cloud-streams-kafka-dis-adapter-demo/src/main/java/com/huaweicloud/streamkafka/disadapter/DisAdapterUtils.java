package com.huaweicloud.streamkafka.disadapter;

import com.huaweicloud.dis.DISConfig;

public class DisAdapterUtils
{
    
    public static DISConfig buildDisConfig(){
        return new DISConfig()
        .setEndpoint("YOUR_ENDPOINT")
        .set("manager.endpoint", "YOUR_ENDPOINT")
        .setAK("YOUR_AK")
        .setSK("YOUR_SK")
        .setProjectId("YOUR_PROJECTID")
        .setRegion("southchina")
        .set("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .set("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        ;
    }
    
}
