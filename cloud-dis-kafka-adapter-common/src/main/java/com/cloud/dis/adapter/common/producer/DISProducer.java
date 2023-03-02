/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloud.dis.adapter.common.producer;

import com.cloud.dis.DISConfig;
import com.cloud.dis.adapter.common.AbstractAdapter;
import com.cloud.dis.adapter.common.model.DisProducerRecord;
import com.cloud.dis.adapter.common.model.ProduceCallback;
import com.cloud.dis.core.handler.AsyncHandler;
import com.cloud.dis.iface.data.request.PutRecordsRequest;
import com.cloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.cloud.dis.iface.data.response.PutRecordsResult;
import com.cloud.dis.iface.stream.request.DescribeStreamRequest;
import com.cloud.dis.iface.stream.response.DescribeStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DISProducer extends AbstractAdapter implements IDISProducer {

    private static final Logger log = LoggerFactory.getLogger(DISProducer.class);

    protected com.cloud.dis.producer.DISProducer producer;


    public DISProducer(DISConfig disConfig) {
        super(disConfig);
        producer = new com.cloud.dis.producer.DISProducer(config, this.disAsync);
    }


    @Override
    public Future<PutRecordsResult> send(DisProducerRecord record) {
        return send(record, null);
    }

    @Override
    public Future<PutRecordsResult> send(DisProducerRecord record, final ProduceCallback callback) {

        String streamName = record.stream();
        String streamId = record.streamId();
        PutRecordsRequest request = new PutRecordsRequest();
        request.setStreamName(streamName);
        request.setStreamId(streamId);

        List<PutRecordsRequestEntry> recordEntries = new ArrayList<PutRecordsRequestEntry>();

        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        if (record.partition() != null) {
            entry.setPartitionId(record.partition().toString());
        }
        entry.setPartitionKey(record.key());
        entry.setData(record.value());
        if (record.timestamp() != null) {
            entry.setTimestamp(record.timestamp());
        }

        recordEntries.add(entry);

        request.setRecords(recordEntries);

        AsyncHandler<PutRecordsResult> asyncHandler = null;
        if (callback != null) {
            asyncHandler = new AsyncHandler<PutRecordsResult>() {
                @Override
                public void onSuccess(PutRecordsResult result) {
                    callback.onCompletion(result, null);
                }

                @Override
                public void onError(Exception exception) {
                    callback.onCompletion(null, exception);
                }
            };
        }

        Future<PutRecordsResult> putResultFuture = null;
        try {
            putResultFuture = producer.putRecordsAsync(request, asyncHandler);
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }

        return putResultFuture;
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public DescribeStreamResult describeStream(String stream) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(stream);
        describeStreamRequest.setLimitPartitions(1);
        return this.disAsync.describeStream(describeStreamRequest);
    }

    @Override
    public void close() {
        this.producer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        this.producer.close(timeout, unit);
    }

    @Override
    protected int getThreadPoolSize() {
        return this.config.getMaxInFlightRequestsPerConnection();
    }
}
