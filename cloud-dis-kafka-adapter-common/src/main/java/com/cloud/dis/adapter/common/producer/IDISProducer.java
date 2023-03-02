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

import com.cloud.dis.adapter.common.model.DisProducerRecord;
import com.cloud.dis.adapter.common.model.ProduceCallback;
import com.cloud.dis.core.DISCredentials;
import com.cloud.dis.iface.data.response.PutRecordsResult;
import com.cloud.dis.iface.stream.response.DescribeStreamResult;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IDISProducer {

    public Future<PutRecordsResult> send(DisProducerRecord record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     */
    public Future<PutRecordsResult> send(DisProducerRecord record, ProduceCallback callback);

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

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     * @param credentials new credentials
     */
    void updateCredentials(DISCredentials credentials);

}
