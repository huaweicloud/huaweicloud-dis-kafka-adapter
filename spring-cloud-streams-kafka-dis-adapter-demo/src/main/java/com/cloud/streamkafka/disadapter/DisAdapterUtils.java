package com.cloud.streamkafka.disadapter;

import com.cloud.dis.DISConfig;

public class DisAdapterUtils
{
    
    public static DISConfig buildDisConfig(){
        return new DISConfig()
        .setEndpoint("https://128.10.96.207:21241")
        .set("manager.endpoint", "https://128.10.96.207:21102")
        .setAK("CWQCEIGWRDFYNYTHXVBD")
        .setSK("hWGwgAsWdqinap7TlgYiqxsGGtP2rADP9UXWPNlV")
        .setProjectId("2fdbbc096fb0420489021acc8b802105")
        .setRegion("southchina")
        .set("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        .set("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .set("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        ;
    }
    
}
