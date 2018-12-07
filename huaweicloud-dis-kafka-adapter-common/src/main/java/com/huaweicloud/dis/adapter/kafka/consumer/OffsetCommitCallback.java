package com.huaweicloud.dis.adapter.kafka.consumer;

import com.huaweicloud.dis.adapter.kafka.model.OffsetAndMetadata;
import com.huaweicloud.dis.adapter.kafka.model.StreamPartition;

import java.util.Map;

public interface OffsetCommitCallback {
    void onComplete(Map<StreamPartition, OffsetAndMetadata> offsets, Exception exception);
}
