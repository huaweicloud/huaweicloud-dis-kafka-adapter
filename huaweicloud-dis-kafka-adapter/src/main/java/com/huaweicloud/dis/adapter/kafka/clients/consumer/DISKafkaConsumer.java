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
package com.huaweicloud.dis.adapter.kafka.clients.consumer;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.consumer.DISConsumer;
import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig;
import com.huaweicloud.dis.adapter.common.consumer.DisNoOpDisConsumerRebalanceListener;
import com.huaweicloud.dis.adapter.common.consumer.DisOffsetAndTimestamp;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.adapter.kafka.clients.ConvertUtils;
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition;
import com.huaweicloud.dis.adapter.kafka.common.PartitionInfo;
import com.huaweicloud.dis.adapter.kafka.common.Node;
import com.huaweicloud.dis.adapter.kafka.common.Metric;
import com.huaweicloud.dis.adapter.kafka.common.MetricName;
import com.huaweicloud.dis.adapter.kafka.common.record.TimestampType;
import com.huaweicloud.dis.adapter.kafka.common.serialization.Deserializer;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A client that consumes records from DIS.
 *
 * <h3>Offsets and Consumer Position</h3>
 * DIS maintains a numerical offset for each record in a partition. This offset acts as a unique identifier of
 * a record within that partition, and also denotes the position of the consumer in the partition. For example, a consumer
 * which is at position 5 has consumed records with offsets 0 through 4 and will next receive the record with offset 5. There
 * are actually two notions of position relevant to the user of the consumer:
 * <p>
 * The {@link #position(TopicPartition) position} of the consumer gives the offset of the next record that will be given
 * out. It will be one larger than the highest offset the consumer has seen in that partition. It automatically advances
 * every time the consumer receives messages in a call to {@link #poll(long)}.
 * <p>
 * The {@link #commitSync() committed position} is the last offset that has been stored securely. Should the
 * process fail and restart, this is the offset that the consumer will recover to. The consumer can either automatically commit
 * offsets periodically; or it can choose to control this committed position manually by calling one of the commit APIs
 * (e.g. {@link #commitSync() commitSync} and {@link #commitAsync(OffsetCommitCallback) commitAsync}).
 * <p>
 * This distinction gives the consumer control over when a record is considered consumed. It is discussed in further
 * detail below.
 *
 * <h3><a name="consumergroups">Consumer Groups and Topic Subscriptions</a></h3>
 *
 * DIS uses the concept of <i>consumer groups</i> to allow a pool of processes to divide the work of consuming and
 * processing records. These processes can either be running on the same machine or they can be
 * distributed over many machines to provide scalability and fault tolerance for processing. All consumer instances
 * sharing the same <code>group.id</code> will be part of the same consumer group.
 * <p>
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to through one of the
 * {@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} APIs. DIS will deliver each message in the
 * subscribed topics to one process in each consumer group. This is achieved by balancing the partitions between all
 * members in the consumer group so that each partition is assigned to exactly one consumer in the group. So if there
 * is a topic with four partitions, and a consumer group with two processes, each process would consume from two partitions.
 * <p>
 * Membership in a consumer group is maintained dynamically: if a process fails, the partitions assigned to it will
 * be reassigned to other consumers in the same group. Similarly, if a new consumer joins the group, partitions will be moved
 * from existing consumers to the new one. This is known as <i>rebalancing</i> the group and is discussed in more
 * detail <a href="#failuredetection">below</a>. Group rebalancing is also used when new partitions are added
 * to one of the subscribed topics or when a new topic matching a {@link #subscribe(Pattern, ConsumerRebalanceListener) subscribed regex}
 * is created. The group will automatically detect the new partitions through periodic heartbeat refreshes and
 * assign them to members of the group.
 * <p>
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens to be made up of
 * multiple processes. As a multi-subscriber system, DIS naturally supports having any number of consumer groups for a
 * given topic without duplicating data (additional consumers are actually quite cheap).
 * <p>
 * This is a slight generalization of the functionality that is common in messaging systems. To get semantics similar to
 * a queue in a traditional messaging system all processes would be part of a single consumer group and hence record
 * delivery would be balanced over the group like with a queue. Unlike a traditional messaging system, though, you can
 * have multiple such groups. To get semantics similar to pub-sub in a traditional messaging system each process would
 * have its own consumer group, so each process would subscribe to all the records published to the topic.
 * <p>
 * In addition, when group reassignment happens automatically, consumers can be notified through a {@link ConsumerRebalanceListener},
 * which allows them to finish necessary application-level logic such as state cleanup, manual offset
 * commits, etc. See <a href="#rebalancecallback">Storing Offsets Outside DIS</a> for more details.
 * <p>
 * It is also possible for the consumer to <a href="#manualassignment">manually assign</a> specific partitions
 * (similar to the older "simple" consumer) using {@link #assign(Collection)}. In this case, dynamic partition
 * assignment and consumer group coordination will be disabled.
 *
 * <h3><a name="failuredetection">Detecting Consumer Failures</a></h3>
 *
 * After subscribing to a set of topics, the consumer will automatically join the group when {@link #poll(long)} is
 * invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call poll, the consumer
 * will stay in the group and continue to receive messages from the partitions it was assigned. Underneath the covers,
 * the consumer sends periodic heartbeats to the server. If the consumer crashes or is unable to send heartbeats for
 * a duration of <code>1 minute</code>, then the consumer will be considered dead and its partitions will
 * be reassigned.
 *
 * <h3>Usage Examples</h3>
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * <h4>Automatic Offset Committing</h4>
 * This example demonstrates a simple usage of DIS's consumer api that relying on automatic offset committing.
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.put(ConsumerConfig.PROPERTY_AK, &quot;YOU_AK&quot;);
 *     props.put(ConsumerConfig.PROPERTY_SK, &quot;YOU_AK&quot;);
 *     props.put(ConsumerConfig.PROPERTY_PROJECT_ID, &quot;YOU_PROJECT_ID&quot;);
 *     props.put(ConsumerConfig.PROPERTY_REGION_ID, &quot;cn-north-1&quot;);
 *     props.put(ConsumerConfig.PROPERTY_ENDPOINT, &quot;https://dis.cn-north-1.myhuaweicloud.com&quot;);
 *     props.put(ConsumerConfig.GROUP_ID_CONFIG, &quot;test&quot;);
 *     props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, &quot;true&quot;);
 *     props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, &quot;1000&quot;);
 *     props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, &quot;com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, &quot;com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer&quot;);
 *     Consumer&lt;String, String&gt; consumer = new DISKafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(5000);
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printfln(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 * Setting <code>ak</code>/<code>sk</code> is the authentication information of your Huawei cloud account,
 * <code>projectId</code> is a collection of resources in your designated <code>region</code> of Huawei cloud, you can
 * get those by visiting <a href="https://console.huaweicloud.com/iam/?region=cn-north-1&locale=zh-cn#/myCredential">My Credentials Page</a>.
 * <p>
 * Setting <code>endpoint</code> is used to connection to the DIS, different <code>region</code> has different <code>endpoint</code>,
 * you can get it by visiting <a href="https://developer.huaweicloud.com/endpoint?DIS">Regions and Endpoints Page</a>.
 * <p>
 * Setting <code>enable.auto.commit</code> means that offsets are committed automatically with a frequency controlled by
 * the config <code>auto.commit.interval.ms</code>.
 * <p>
 * In this example the consumer is subscribing to the stream <i>foo</i> and <i>bar</i> as part of a group of consumers
 * called <i>test</i> as configured with <code>group.id</code>.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying string deserializers, we
 * are saying that our record's key and value will just be simple strings.
 *
 * <h4>Manual Offset Control</h4>
 *
 * Instead of relying on the consumer to periodically commit consumed offsets, users can also control when records
 * should be considered as consumed and hence commit their offsets. This is useful when the consumption of the messages
 * is coupled with some processing logic and hence a message should not be considered as consumed until it is completed processing.

 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.put(ConsumerConfig.PROPERTY_AK, &quot;YOU_AK&quot;);
 *     props.put(ConsumerConfig.PROPERTY_SK, &quot;YOU_AK&quot;);
 *     props.put(ConsumerConfig.PROPERTY_PROJECT_ID, &quot;YOU_PROJECT_ID&quot;);
 *     props.put(ConsumerConfig.PROPERTY_REGION_ID, &quot;cn-north-1&quot;);
 *     props.put(ConsumerConfig.PROPERTY_ENDPOINT, &quot;https://dis.cn-north-1.myhuaweicloud.com&quot;);
 *     props.put(ConsumerConfig.GROUP_ID_CONFIG, &quot;test&quot;);
 *     props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, &quot;false&quot;);
 *     props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, &quot;com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer&quot;);
 *     props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, &quot;com.huaweicloud.dis.adapter.kafka.common.serialization.StringDeserializer&quot;);
 *     Consumer&lt;String, String&gt; consumer = new DISKafkaConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     final int minBatchSize = 200;
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(5000);
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *         }
 *         if (buffer.size() &gt;= minBatchSize) {
 *             insertIntoDb(buffer);
 *             consumer.commitSync();
 *             buffer.clear();
 *         }
 *     }
 * </pre>
 *
 * In this example we will consume a batch of records and batch them up in memory. When we have enough records
 * batched, we will insert them into a database. If we allowed offsets to auto commit as in the previous example, records
 * would be considered consumed after they were returned to the user in {@link #poll(long) poll}. It would then be possible
 * for our process to fail after batching the records, but before they had been inserted into the database.
 * <p>
 * To avoid this, we will manually commit the offsets only after the corresponding records have been inserted into the
 * database. This gives us exact control of when a record is considered consumed. This raises the opposite possibility:
 * the process could fail in the interval after the insert into the database but before the commit (even though this
 * would likely just be a few milliseconds, it is a possibility). In this case the process that took over consumption
 * would consume from last committed offset and would repeat the insert of the last batch of data. Used in this way
 * DIS provides what is often called "at-least-once" delivery guarantees, as each record will likely be delivered one
 * time but in failure cases could be duplicated.
 * <p>
 * <b>Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that
 * you must consume all data returned from each call to {@link #poll(long)} before any subsequent calls, or before
 * {@link #close() closing} the consumer. If you fail to do either of these, it is possible for the committed offset
 * to get ahead of the consumed position, which results in missing records. The advantage of using manual offset
 * control is that you have direct control over when a record is considered "consumed."</b>
 * <p>
 * The above example uses {@link #commitSync() commitSync} to mark all received records as committed. In some cases
 * you may wish to have even finer control over which records have been committed by specifying an offset explicitly.
 * In the example below we commit offset after we finish handling the records in each partition.
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(5000);
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * <b>Note: The committed offset should always be the offset of the next message that your application will read.</b>
 * Thus, when calling {@link #commitSync(Map) commitSync(offsets)} you should add one to the offset of the last message processed.
 *
 * <h4><a name="manualassignment">Manual Partition Assignment</a></h4>
 *
 * In the previous examples, we subscribed to the topics we were interested in and let DIS dynamically assign a
 * fair share of the partitions for those topics based on the active consumers in the group. However, in
 * some cases you may need finer control over the specific partitions that are assigned. For example:
 * <p>
 * <ul>
 * <li>If the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store), then it should only get records for the partition it is maintaining on disk.
 * <li>If the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for DIS to detect the failure and reassign the partition since the consuming process
 * will be restarted on another machine.
 * </ul>
 * <p>
 * To use this mode, instead of subscribing to the topic using {@link #subscribe(Collection) subscribe}, you just call
 * {@link #assign(Collection)} with the full list of partitions that you want to consume.
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 *
 * Once assigned, you can call {@link #poll(long) poll} in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions
 * will only change with another call to {@link #assign(Collection) assign}. Manual partition assignment does
 * not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer
 * acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should
 * usually ensure that the groupId is unique for each consumer instance.
 * <p>
 * Note that it isn't possible to mix manual partition assignment (i.e. using {@link #assign(Collection) assign})
 * with dynamic partition assignment through topic subscription (i.e. using {@link #subscribe(Collection) subscribe}).
 *
 * <h4><a name="rebalancecallback">Storing Offsets Outside DIS</h4>
 *
 * The consumer application need not use DIS's built-in offset storage, it can store offsets in a store of its own
 * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
 * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
 * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
 * stronger than the default "at-least once" semantics you get with DIS's offset commit functionality.
 * <p>
 * Here are a couple of examples of this type of usage:
 * <ul>
 * <li>If the results of the consumption are being stored in a relational database, storing the offset in the database
 * as well can allow committing both the results and offset in a single transaction. Thus either the transaction will
 * succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset
 * won't be updated.
 * <li>If the results are being stored in a local store it may be possible to store the offset there as well. For
 * example a search index could be built by subscribing to a particular partition and storing both the offset and the
 * indexed data together. If this is done in a way that is atomic, it is often possible to have it be the case that even
 * if a crash occurs that causes unsync'd data to be lost, whatever is left has the corresponding offset stored as well.
 * This means that in this case the indexing process that comes back having lost recent updates just resumes indexing
 * from what it has ensuring that no updates are lost.
 * </ul>
 * <p>
 * Each record comes with its own offset, so to manage your own offset you just need to do the following:
 *
 * <ul>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ul>
 *
 * <p>
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care is
 * needed to handle the case where partition assignments change. This can be done by providing a
 * {@link ConsumerRebalanceListener} instance in the call to {@link #subscribe(Collection, ConsumerRebalanceListener)}
 * and {@link #subscribe(Pattern, ConsumerRebalanceListener)}.
 * For example, when partitions are taken from a consumer the consumer will want to commit its offset for those partitions by
 * implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}.
 * <p>
 * Another common use for {@link ConsumerRebalanceListener} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 *
 * <h4>Controlling The Consumer's Position</h4>
 *
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing its
 * position (either automatically or manually). However DIS allows the consumer to manually control its position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * <p>
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * <p>
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize its position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * re-consuming all the data and recreating the state (assuming that DIS is retaining sufficient history).
 * <p>
 * DIS allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(Collection)} and {@link #seekToEnd(Collection)} respectively).
 *
 * <h4>Consumption Flow Control</h4>
 *
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
 * effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
 * first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
 * when these partitions have few or no data to consume.
 *
 * <p>
 * One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams.
 * When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic
 * in order to get the lagging stream to catch up. Another example is bootstraping upon consumer starting up where there are
 * a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider
 * fetching other topics.
 *
 * <p>
 * DIS supports dynamic controlling of consumption flows by using {@link #pause(Collection)} and {@link #resume(Collection)}
 * to pause the consumption on the specified assigned partitions and resume the consumption
 * on the specified paused partitions respectively in the future {@link #poll(long)} calls.
 */

public class DISKafkaConsumer<K, V> implements Consumer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DISKafkaConsumer.class);
    private static final Node[] EMPTY_NODES = new Node[0];

    private DISConsumer disConsumer;

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    public DISKafkaConsumer(Map configs) {
        this(newDisConfig(configs));
    }

    public DISKafkaConsumer(Map<String, Object> configs,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        this(newDisConfig(configs), keyDeserializer, valueDeserializer);
    }

    public DISKafkaConsumer(Properties properties) {
        this((Map) properties);
    }

    public DISKafkaConsumer(Properties properties,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        this((Map) properties, keyDeserializer, valueDeserializer);
    }

    public DISKafkaConsumer(DISConfig disConfig) {
        this(disConfig, null, null);
    }

    public DISKafkaConsumer(DISConfig disConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        disConsumer = new DISConsumer(disConfig);
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        if (keyDeserializer == null) {
            Class keyDeserializerClass = StringDeserializer.class;
            String className = (String) disConfig.get(DisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            if (className != null) {
                try {
                    keyDeserializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                    return;
                }
            }
            try {
                this.keyDeserializer = (Deserializer<K>) keyDeserializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
                return;
            }
        }

        if (valueDeserializer == null) {
            Class valueDeserializerClass = StringDeserializer.class;
            String className = (String) disConfig.get(DisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            if (className != null) {
                try {
                    valueDeserializerClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    log.error(e.getMessage());
                    return;
                }
            }
            try {
                this.valueDeserializer = (Deserializer<V>) valueDeserializerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
                return;
            }
        }
        log.debug("create DISKafkaConsumer successfully");
    }

    private static DISConfig newDisConfig(Map map) {
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(map);
        return disConfig;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return ConvertUtils.convert2TopicPartitionSet(disConsumer.assignment());
    }

    @Override
    public Set<String> subscription() {
        return disConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(collection, ConvertUtils.convert2DisConsumerRebalanceListener(consumerRebalanceListener));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        disConsumer.subscribe(topics, new DisNoOpDisConsumerRebalanceListener());
    }

    /**
     *
     *  @param topicNames 通道名称（租户自己创建自己消费场景）
     * @param topicIds 通道ID列表（跨账号进行消费场景）
     */
    @Override
    public void subscribe(Collection<String> topicNames, Collection<String> topicIds) {
        disConsumer.subscribe(topicNames, topicIds, new DisNoOpDisConsumerRebalanceListener());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        disConsumer.assign(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        disConsumer.subscribe(pattern, ConvertUtils.convert2DisConsumerRebalanceListener(consumerRebalanceListener));
    }

    @Override
    public void unsubscribe() {
        disConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> res = new HashMap<>();
        Map<StreamPartition, List<Record>> records = disConsumer.poll(timeout);
        for (Map.Entry<StreamPartition, List<Record>> entry : records.entrySet()) {
            TopicPartition partition = new TopicPartition(entry.getKey().stream(), entry.getKey().partition());
            for (Record record : entry.getValue()) {
                K key = record.getPartitionKey() == null ? null
                        : keyDeserializer.deserialize(entry.getKey().stream(), record.getPartitionKey().getBytes());
                V value = valueDeserializer.deserialize(entry.getKey().stream(), record.getData().array());
                ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<K, V>(entry.getKey().stream(),
                        entry.getKey().partition(), Long.valueOf(record.getSequenceNumber()), record.getTimestamp(),
                        TimestampType.forName(record.getTimestampType()), 0L, 0, 0, key, value);
                res.putIfAbsent(partition, new ArrayList<>());
                res.get(partition).add(consumerRecord);
            }
        }
        return new ConsumerRecords<K, V>(res);
    }

    @Override
    public void commitSync() {
        disConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        disConsumer.commitSync(ConvertUtils.convert2DisOffsetAndMetadataMap(offsets));
    }

    @Override
    public void commitAsync() {
        disConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        disConsumer.commitAsync(ConvertUtils.convert2DisOffsetCommitCallback(offsetCommitCallback));
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback offsetCommitCallback) {
        disConsumer.commitAsync(ConvertUtils.convert2DisOffsetAndMetadataMap(offsets),
                ConvertUtils.convert2DisOffsetCommitCallback(offsetCommitCallback));
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        disConsumer.seek(ConvertUtils.convert2StreamPartition(partition), offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        disConsumer.seekToBeginning(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        disConsumer.seekToEnd(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return disConsumer.position(ConvertUtils.convert2StreamPartition(partition));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return ConvertUtils.convert2OffsetAndMetadata(
                disConsumer.committed(ConvertUtils.convert2StreamPartition(partition)));
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        DescribeStreamResult describeStreamResult = disConsumer.describeStream(topic);
        for (int i = 0; i < describeStreamResult.getReadablePartitionCount(); i++) {
            partitionInfos.add(new PartitionInfo(topic, i, Node.noNode(), EMPTY_NODES, EMPTY_NODES));
        }
        return partitionInfos;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        List<DescribeStreamResult> results = disConsumer.listStreams();
        if (results == null) {
            return null;
        }
        Map<String, List<PartitionInfo>> map = new HashMap<>();
        for (DescribeStreamResult describeStreamResult : results) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (int i = 0; i < describeStreamResult.getReadablePartitionCount(); i++) {
                partitionInfos.add(new PartitionInfo(describeStreamResult.getStreamName(), i, Node.noNode(), EMPTY_NODES, EMPTY_NODES));
            }
            map.put(describeStreamResult.getStreamName(), partitionInfos);
        }
        return map;
    }

    @Override
    public Set<TopicPartition> paused() {
        return ConvertUtils.convert2TopicPartitionSet(disConsumer.paused());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        disConsumer.pause(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        disConsumer.resume(ConvertUtils.convert2StreamPartitionCollection(partitions));
    }

    @Override
    public void close() {
        disConsumer.close();
    }

    @Override
    public void wakeup() {
        disConsumer.wakeup();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        close();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map) {
        Map<StreamPartition, DisOffsetAndTimestamp> offsets = disConsumer.offsetsForTimes(ConvertUtils.convert2StreamPartitionLongMap(map));
        Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>();
        for (Map.Entry<StreamPartition, DisOffsetAndTimestamp> entry : offsets.entrySet()) {
            results.put(ConvertUtils.convert2TopicPartition(entry.getKey()), new OffsetAndTimestamp(entry.getValue().offset(), entry.getValue().timestamp()));
        }
        return results;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        return ConvertUtils.convert2TopicPartitionLongMap(
                disConsumer.beginningOffsets(ConvertUtils.convert2StreamPartitionCollection(collection)));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        return ConvertUtils.convert2TopicPartitionLongMap(
                disConsumer.endOffsets(ConvertUtils.convert2StreamPartitionCollection(collection)));
    }

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     *
     * @param credentials new credentials
     */
    public void updateCredentials(DISCredentials credentials) {
        disConsumer.updateCredentials(credentials);
    }

    public void updateAuthToken(String authToken){
        disConsumer.updateAuthToken(authToken);
    }
}
