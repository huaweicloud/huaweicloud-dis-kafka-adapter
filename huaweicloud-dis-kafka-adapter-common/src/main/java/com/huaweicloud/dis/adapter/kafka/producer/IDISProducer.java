package com.huaweicloud.dis.adapter.kafka.producer;

import com.huaweicloud.dis.adapter.kafka.model.ProduceCallback;
import com.huaweicloud.dis.adapter.kafka.model.ProducerRecord;
import com.huaweicloud.dis.iface.data.response.PutRecordResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.producer.internals.ProduceRequestResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IDISProducer {

    public Future<PutRecordsResult> send(ProducerRecord record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     */
    public Future<PutRecordsResult> send(ProducerRecord record, ProduceCallback callback);

    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     */
    public DescribeStreamResult describeStream(String stream);

    /**
     * Close this producer
     */
    public void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);


}
