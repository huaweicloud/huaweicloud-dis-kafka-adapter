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

package com.huaweicloud.dis.adapter.common.consumer;

import com.huaweicloud.dis.adapter.common.model.DisOffsetAndMetadata;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public interface IDISConsumer extends Cloneable {

    public Set<StreamPartition> assignment();

    public Set<String> subscription();


    public void subscribe(Collection<String> topics);


    public void subscribe(Collection<String> topics, DisConsumerRebalanceListener callback);


    public void assign(Collection<StreamPartition> partitions);


    public void subscribe(Pattern pattern, DisConsumerRebalanceListener callback);

    public void unsubscribe();

    public Map<StreamPartition, List<Record>> poll(long timeout);

    public void commitSync();

    public void commitSync(Map<StreamPartition, DisOffsetAndMetadata> offsets);

    public void commitAsync();

    public void commitAsync(DisOffsetCommitCallback callback);

    public void commitAsync(Map<StreamPartition, DisOffsetAndMetadata> offsets, DisOffsetCommitCallback callback);

    public void seek(StreamPartition partition, long offset);

    public void seekToBeginning(Collection<StreamPartition> partitions);

    public void seekToEnd(Collection<StreamPartition> partitions);

    public long position(StreamPartition partition);

    public DisOffsetAndMetadata committed(StreamPartition partition);

    public DescribeStreamResult describeStream(String stream);

    public List<DescribeStreamResult> listStreams();

    public Set<StreamPartition> paused();

    public void pause(Collection<StreamPartition> partitions);

    public void resume(Collection<StreamPartition> partitions);

    public void close();

    public void wakeup();

    public Map<StreamPartition, DisOffsetAndTimestamp> offsetsForTimes(Map<StreamPartition, Long> map);

    public Map<StreamPartition, Long> beginningOffsets(Collection<StreamPartition> collection);

    public Map<StreamPartition, Long> endOffsets(Collection<StreamPartition> collection);
}
