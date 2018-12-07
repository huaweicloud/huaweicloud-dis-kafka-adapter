package com.huaweicloud.dis.adapter.kafka.consumer;

import com.huaweicloud.dis.adapter.kafka.model.StreamPartition;

public class NoOffsetForPartitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NoOffsetForPartitionException(StreamPartition partition) {
        super("Undefined offset with no reset policy for partition: " + partition);

    }
}
