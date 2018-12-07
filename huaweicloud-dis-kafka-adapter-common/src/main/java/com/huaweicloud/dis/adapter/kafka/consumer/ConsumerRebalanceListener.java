package com.huaweicloud.dis.adapter.kafka.consumer;

import com.huaweicloud.dis.adapter.kafka.model.StreamPartition;

import java.util.Collection;

public interface ConsumerRebalanceListener {

    void onPartitionsRevoked(Collection<StreamPartition> partitions);

    void onPartitionsAssigned(Collection<StreamPartition> partitions);

}
