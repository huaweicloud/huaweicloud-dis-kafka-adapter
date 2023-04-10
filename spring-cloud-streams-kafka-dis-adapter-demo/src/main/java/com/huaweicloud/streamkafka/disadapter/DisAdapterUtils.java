package com.huaweicloud.streamkafka.disadapter;

import com.huaweicloud.dis.DISConfig;

public class DisAdapterUtils
{
    
    public static DISConfig buildDisConfig(){
        return new DISConfig()
        .setEndpoint("your endpoint")
        .set("manager.endpoint", "your manager endpoint")
        .setAK("your ak")
        .setSK("your sk")
        .setProjectId("your projectId")
        .setRegion("your region")
        .set("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .set("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        ;
    }
    
}
