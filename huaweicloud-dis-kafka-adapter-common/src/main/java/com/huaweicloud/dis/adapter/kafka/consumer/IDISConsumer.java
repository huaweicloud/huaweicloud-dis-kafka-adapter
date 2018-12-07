package com.huaweicloud.dis.adapter.kafka.consumer;

import com.huaweicloud.dis.adapter.kafka.model.OffsetAndMetadata;
import com.huaweicloud.dis.adapter.kafka.model.StreamPartition;
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


    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);


    public void assign(Collection<StreamPartition> partitions);


    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    public void unsubscribe();

    public Map<StreamPartition,List<Record>> poll(long timeout);

    public void commitSync();

    public void commitSync(Map<StreamPartition, OffsetAndMetadata> offsets);

    public void commitAsync();

    public void commitAsync(OffsetCommitCallback callback);

    public void commitAsync(Map<StreamPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    public void seek(StreamPartition partition, long offset);

    public void seekToBeginning(Collection<StreamPartition> partitions);

    public void seekToEnd(Collection<StreamPartition> partitions);

    public long position(StreamPartition partition);

    public OffsetAndMetadata committed(StreamPartition partition);

    public DescribeStreamResult describeStream(String stream);

    public List<DescribeStreamResult> listStreams();

    public Set<StreamPartition> paused();

    public void pause(Collection<StreamPartition> partitions);

    public void resume(Collection<StreamPartition> partitions);

    public void close();

    public void wakeup();
}
