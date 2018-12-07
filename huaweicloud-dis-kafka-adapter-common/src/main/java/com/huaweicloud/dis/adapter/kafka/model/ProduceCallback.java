package com.huaweicloud.dis.adapter.kafka.model;

import com.huaweicloud.dis.iface.data.response.PutRecordsResult;

public interface ProduceCallback {
    public void onCompletion(PutRecordsResult result, Exception exception);

}
