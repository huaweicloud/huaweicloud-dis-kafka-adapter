package com.huaweicloud.dis.adapter.kafka.consumer;

import com.huaweicloud.dis.adapter.kafka.model.StreamPartition;

import java.util.Collection;

public class NoOpConsumerRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsAssigned(Collection<StreamPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<StreamPartition> partitions) {}

}
