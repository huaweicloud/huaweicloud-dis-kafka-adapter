/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.common.consumer;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.AbstractAdapter;
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.model.DisOffsetAndMetadata;
import com.huaweicloud.dis.adapter.common.model.DisOffsetResetStrategy;
import com.huaweicloud.dis.adapter.common.model.PartitionIterator;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.exception.DISTimestampOutOfRangeException;
import com.huaweicloud.dis.http.exception.HttpClientErrorException;
import com.huaweicloud.dis.http.exception.RestClientException;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.Record;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.request.ListStreamsRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.ListStreamsResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import com.huaweicloud.dis.util.JsonUtils;
import com.huaweicloud.dis.util.PartitionCursorTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;


public class DISConsumer extends AbstractAdapter implements IDISConsumer {
    private static final Logger log = LoggerFactory.getLogger(DISConsumer.class);
    private static final long NO_CURRENT_THREAD = -1L;

    private static final long MAX_POLL_TIMEOUT = 30000L;
    private Coordinator coordinator;
    private SubscriptionState subscriptions;
    private boolean closed = false;
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refcount = new AtomicInteger(0);

    private String clientId;
    private String groupId;
    private Fetcher fetcher;
    private boolean forceWakeup = false;
    ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators;

    public DISConsumer(Map configs) {
        this(Utils.newDisConfig(configs));
    }

    public DISConsumer(DISConfig disConfig) {
        super(disConfig);
        log.trace("Starting the DIS consumer");
        this.clientId = disConfig.get(DisConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + UUID.randomUUID());
        this.groupId = disConfig.get(DisConsumerConfig.GROUP_ID_CONFIG, "");
        DisOffsetResetStrategy disOffsetResetStrategy = DisOffsetResetStrategy.valueOf(
                disConfig.get(DisConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DisOffsetResetStrategy.LATEST.name()).toUpperCase());
        this.subscriptions = new SubscriptionState(disOffsetResetStrategy);
        this.nextIterators = new ConcurrentHashMap<>();
        boolean autoCommitEnabled = disConfig.getBoolean(DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        long autoCommitIntervalMs = Long.valueOf(disConfig.get(DisConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"));

        this.coordinator = new Coordinator(this.disAsync,
                this.clientId,
                this.groupId,
                this.subscriptions,
                autoCommitEnabled,
                autoCommitIntervalMs,
                this.nextIterators,
                disConfig);
        this.fetcher = new Fetcher(this.disAsync,
                this.config,
                this.subscriptions,
                this.coordinator,
                this.nextIterators);
        log.info("DIS consumer created");
    }

    @Override
    public Set<StreamPartition> assignment() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.assignedPartitions()));
        } finally {
            release();
        }
    }

    @Override
    public Set<String> subscription() {
        acquire();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> streams, DisConsumerRebalanceListener listener) {
        acquire();
        try {
            if (streams.isEmpty()) {
                this.unsubscribe();
            } else {
                log.debug("Subscribed to stream(s): {}", Utils.join(streams, ", "));
                this.subscriptions.subscribe(streams, listener);
            }
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> streams) {
        subscribe(streams, new DisNoOpDisConsumerRebalanceListener());
    }

    @Override
    public void assign(Collection<StreamPartition> partitions) {
        acquire();
        try {
            log.debug("Subscribed to partition(s): {}", Utils.join(partitions, ", "));
            this.subscriptions.assignFromUser(partitions);
            //   this.subscriptions.commitsRefreshed();
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern, DisConsumerRebalanceListener callback) {
        acquire();
        try {
            log.debug("Subscribed to pattern: {}", pattern);
            this.subscriptions.subscribe(pattern, callback);
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe() {
        acquire();
        try {
            log.debug("Unsubscribed all streams or patterns and assigned partitions");
            this.subscriptions.unsubscribe();
            coordinator.maybeLeaveGroup();
        } finally {
            release();
        }
    }

    @Override
    public Map<StreamPartition, List<Record>> poll(long timeout) {
        Map<StreamPartition, List<Record>> partitionRecords = null;
        boolean autoRetry = this.config.getBoolean(DisConsumerConfig.ENABLE_EXCEPTION_AUTO_RETRY_CONFIG, true);
        if (!autoRetry) {
            partitionRecords = innerPoll(timeout);
        } else {
            // 如果自动重试开启，则重试，保证进程不因为此异常而终止
            long totalRetryNum = Long.valueOf(this.config.get(DisConsumerConfig.PROPERTY_EXCEPTION_RETRY_NUM, "60"));
            long retryWaitTime = Long.valueOf(this.config.get(DisConsumerConfig.PROPERTY_EXCEPTION_RETRY_WAIT_TIME_MS, "60000"));
            long retryCount = 0;
            boolean flag = true;
            while (flag) {
                try {
                    partitionRecords = innerPoll(timeout);
                    break;
                } catch (Throwable t) {
                    if (isRetriableException(t)) {
                        retryCount++;
                        if (retryCount > totalRetryNum) {
                            flag = false;
                        }
                        log.warn("Failed to poll, currRetryCount is {}, wait for {} ms", retryCount, retryWaitTime, t);
                        try {
                            Thread.sleep(retryWaitTime);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        // 对于不需要重试的异常，直接将异常抛出
                        throw t;
                    }
                }
            }
        }

        return partitionRecords;
    }

    protected boolean isRetriableException(Throwable t)
    {
        // 对于客户端类异常，不进行重试
        if (t.getCause() == null) {
            return !(t instanceof IllegalArgumentException) && !(t instanceof HttpClientErrorException);
        } else {
            return isRetriableException(t.getCause());
        }
    }

    private Map<StreamPartition, List<Record>> innerPoll(long timeout) {
        acquire();
        try {
            if (timeout < 0) {
                throw new IllegalArgumentException("Timeout must not be negative");
            }
            if (timeout > MAX_POLL_TIMEOUT && subscriptions.partitionsAutoAssigned()) {
                // timeout should not be too long when using consumer group.
                timeout = MAX_POLL_TIMEOUT;
            }

            long endTime = System.currentTimeMillis() + timeout;

            // throws exception when some exceptions occurs in fetcher.fetchRecords()
            fetcher.throwIfFetchError();
            coordinator.executeDelayedTask();
            if (subscriptions.partitionsAutoAssigned()) {
                coordinator.ensureGroupStable();
            }
            Fetcher.FetchResult fetchResult;
            forceWakeup = false;
            long remaining = endTime - System.currentTimeMillis();
            do {
                fetcher.sendFetchRequests();
                fetchResult = fetcher.fetchRecords(remaining);
                remaining = endTime - System.currentTimeMillis();
            }
            while (fetchResult.getRecordsCount() == 0 && remaining > 0 && !forceWakeup);
            return fetchResult.getRecords();
        } finally {
            release();
        }
    }

    @Override
    public void commitSync() {
        acquire();
        try {
            commitSync(subscriptions.allConsumed());
        } finally {
            release();
        }
    }

    @Override
    public void commitSync(Map<StreamPartition, DisOffsetAndMetadata> offsets) {
        acquire();
        try {
            coordinator.commitSync(offsets);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(DisOffsetCommitCallback callback) {
        acquire();
        try {
            commitAsync(subscriptions.allConsumed(), callback);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync(Map<StreamPartition, DisOffsetAndMetadata> offsets, DisOffsetCommitCallback callback) {
        acquire();
        try {
            log.debug("Committing offsets: {} ", offsets);
            coordinator.commitAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    @Override
    public void seek(StreamPartition partition, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        acquire();
        try {
            log.debug("Seeking to offset {} for partition {}", offset, partition);
            this.subscriptions.seek(partition, offset);
            nextIterators.remove(partition);
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<StreamPartition> partitions) {
        acquire();
        try {
            Collection<StreamPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (StreamPartition tp : parts) {
                log.debug("Seeking to beginning of partition {}", tp);
                subscriptions.needOffsetReset(tp, DisOffsetResetStrategy.EARLIEST);
            }
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<StreamPartition> partitions) {
        acquire();
        try {
            Collection<StreamPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            for (StreamPartition tp : parts) {
                log.debug("Seeking to end of partition {}", tp);
                subscriptions.needOffsetReset(tp, DisOffsetResetStrategy.LATEST);
            }
        } finally {
            release();
        }
    }

    @Override
    public long position(StreamPartition partition) {
        acquire();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
            Set<StreamPartition> needUpdatePositionPartition = new HashSet<>();
            for (StreamPartition part : this.subscriptions.assignedPartitions()) {
                if (this.subscriptions.position(part) == null) {
                    needUpdatePositionPartition.add(part);
                }
            }
            if (!needUpdatePositionPartition.isEmpty()) {
                coordinator.updateFetchPositions(needUpdatePositionPartition);
            }
            return this.subscriptions.position(partition);
        } finally {
            release();
        }
    }

    @Override
    public DisOffsetAndMetadata committed(StreamPartition partition) {
        acquire();
        try {
            DisOffsetAndMetadata committed;
            if (subscriptions.isAssigned(partition)) {
                committed = this.subscriptions.committed(partition);
                if (committed == null || subscriptions.refreshCommitsNeeded()) {
                    coordinator.refreshCommittedOffsetsIfNeeded();
                    committed = this.subscriptions.committed(partition);
                }
            } else {
                Map<StreamPartition, DisOffsetAndMetadata> offsets = coordinator.fetchCommittedOffsets(Collections.singleton(partition));
                committed = offsets.get(partition);
            }
            return committed;
        } finally {
            release();
        }
    }

    @Override
    public DescribeStreamResult describeStream(String stream) {
        acquire();
        try {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName(stream);
            describeStreamRequest.setLimitPartitions(1);
            DescribeStreamResult describeStreamResult = disAsync.describeStream(describeStreamRequest);
            return describeStreamResult;
        } finally {
            release();
        }
    }

    @Override
    public List<DescribeStreamResult> listStreams() {
        acquire();
        List<DescribeStreamResult> results = new ArrayList<>();
        try {
            int limit = 100;
            String startStreamName = "";
            while (true) {
                ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
                listStreamsRequest.setLimit(limit);
                listStreamsRequest.setExclusiveStartStreamName(startStreamName);
                ListStreamsResult listStreamsResult = disAsync.listStreams(listStreamsRequest);
                if (listStreamsResult == null || listStreamsResult.getStreamNames() == null) {
                    break;
                }
                List<String> streams = listStreamsResult.getStreamNames();
                for (String stream : streams) {
                    DescribeStreamResult describeStreamResult = describeStream(stream);
                    results.add(describeStreamResult);
                }
                if (!listStreamsResult.getHasMoreStreams()) {
                    break;
                }
                startStreamName = streams.get(streams.size() - 1);
            }
        } finally {
            release();
        }
        return results;
    }

    @Override
    public Set<StreamPartition> paused() {
        acquire();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<StreamPartition> partitions) {
        acquire();
        try {
            for (StreamPartition partition : partitions) {
                log.debug("Pausing partition {}", partition);
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<StreamPartition> partitions) {
        acquire();
        try {
            for (StreamPartition partition : partitions) {
                log.debug("Resuming partition {}", partition);
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void close() {
        acquire();
        try {
            log.debug("Closing the Kafka consumer.");
            closed = true;
            wakeup();
            this.coordinator.close();
            super.close();
            log.debug("The DIS consumer has closed.");
        } finally {
            release();
        }
    }

    @Override
    public void wakeup() {
        forceWakeup = true;
        fetcher.wakeup();
    }

    private void acquire() {
        ensureNotClosed();
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException("DisConsumer is not safe for multi-threaded access");
        }
        refcount.incrementAndGet();
    }

    private void ensureNotClosed() {
        if (this.closed) {
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    private void release() {
        if (refcount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public Map<StreamPartition, DisOffsetAndTimestamp> offsetsForTimes(Map<StreamPartition, Long> map) {
        if (map == null) {
            throw new IllegalArgumentException("timestamp should not be null");
        }

        for (Map.Entry<StreamPartition, Long> entry : map.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time should great than 0.");
            }
        }

        Map<StreamPartition, DisOffsetAndTimestamp> results = new ConcurrentHashMap<>();
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        final CountDownLatch countDownLatch = new CountDownLatch(map.size());
        for (Map.Entry<StreamPartition, Long> entry : map.entrySet()) {
            StreamPartition partition = entry.getKey();
            long timestamp = entry.getValue();
            GetPartitionCursorRequest getPartitionCursorRequest = new GetPartitionCursorRequest();
            getPartitionCursorRequest.setCursorType(PartitionCursorTypeEnum.AT_TIMESTAMP.name());
            getPartitionCursorRequest.setStreamName(partition.stream());
            getPartitionCursorRequest.setPartitionId(String.valueOf(partition.partition()));
            getPartitionCursorRequest.setTimestamp(timestamp);

            disAsync.getPartitionCursorAsync(getPartitionCursorRequest, new AsyncHandler<GetPartitionCursorResult>() {
                @Override
                public void onError(Exception e) {
                    // timestamp is expired
                    if (e instanceof DISTimestampOutOfRangeException) {
                        long earliestTimestamp = ((DISTimestampOutOfRangeException) e).getEarliestTimestamp();
                        log.warn("{} timestamp {} is expired, will starts from the earliest timestamp {}", partition, timestamp, earliestTimestamp);
                        GetPartitionCursorRequest reGetPartitionCursorRequest = new GetPartitionCursorRequest();
                        reGetPartitionCursorRequest.setCursorType(PartitionCursorTypeEnum.AT_TIMESTAMP.name());
                        reGetPartitionCursorRequest.setStreamName(partition.stream());
                        reGetPartitionCursorRequest.setPartitionId(String.valueOf(partition.partition()));
                        reGetPartitionCursorRequest.setTimestamp(earliestTimestamp);
                        disAsync.getPartitionCursorAsync(reGetPartitionCursorRequest, new AsyncHandler<GetPartitionCursorResult>() {
                            @Override
                            public void onError(Exception e) {
                                exceptionAtomicReference.set(e);
                                countDownLatch.countDown();
                                log.error("Failed to reGetPartitionCursorAsync, error : [{}], request : [{}]",
                                        e.getMessage(), JsonUtils.objToJson(getPartitionCursorRequest));
                            }

                            @Override
                            public void onSuccess(GetPartitionCursorResult getPartitionCursorResult) {
                                try {
                                    PartitionIterator partitionIterator = Utils.decodeIterator(getPartitionCursorResult.getPartitionCursor());
                                    results.put(partition, new DisOffsetAndTimestamp(
                                            Long.valueOf(partitionIterator.getGetIteratorParam().getStartingSequenceNumber()), earliestTimestamp));
                                } catch (Exception e) {
                                    exceptionAtomicReference.set(e);
                                    log.error(e.getMessage(), e);
                                } finally {
                                    countDownLatch.countDown();
                                }
                            }
                        });
                    } else {
                        exceptionAtomicReference.set(e);
                        countDownLatch.countDown();
                        log.error("Failed to getPartitionCursorAsync, error : [{}], request : [{}]",
                                e.getMessage(), JsonUtils.objToJson(getPartitionCursorRequest));
                    }
                }

                @Override
                public void onSuccess(GetPartitionCursorResult getPartitionCursorResult) {
                    try {
                        PartitionIterator partitionIterator = Utils.decodeIterator(getPartitionCursorResult.getPartitionCursor());
                        results.put(partition, new DisOffsetAndTimestamp(
                                Long.valueOf(partitionIterator.getGetIteratorParam().getStartingSequenceNumber()), timestamp));
                    } catch (Exception e) {
                        exceptionAtomicReference.set(e);
                        log.error(e.getMessage(), e);
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        Exception e = exceptionAtomicReference.get();
        if (e != null) {
            if (e instanceof DISClientException) {
                throw (DISClientException) e;
            } else {
                throw new DISClientException(e);
            }
        }

        return results;
    }

    private Map<StreamPartition, Long> offsets(Collection<StreamPartition> collection, boolean beginningOrEnd) {
        Map<String, List<Integer>> describeTopic = new HashMap<>();
        Map<StreamPartition, Long> results = new HashMap<>();
        for (StreamPartition streamPartition : collection) {
            if (describeTopic.get(streamPartition.stream()) == null) {
                describeTopic.putIfAbsent(streamPartition.stream(), new ArrayList<>());
            }
            describeTopic.get(streamPartition.stream()).add(streamPartition.partition());
        }
        for (Map.Entry<String, List<Integer>> entry : describeTopic.entrySet()) {
            List<Integer> parts = entry.getValue();
            parts.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return Integer.compare(o1, o2);
                }
            });
            int index = 0;
            while (index < parts.size()) {
                final int LIMIT = 100;
                String partitionId = parts.get(index) == 0 ? "" : Utils.getShardIdStringFromPartitionId(parts.get(index) - 1);
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(entry.getKey());
                describeStreamRequest.setLimitPartitions(LIMIT);
                describeStreamRequest.setStartPartitionId(partitionId);
                DescribeStreamResult describeStreamResult = disAsync.describeStream(describeStreamRequest);
                for (PartitionResult partitionResult : describeStreamResult.getPartitions()) {
                    if (Utils.getKafkaPartitionFromPartitionId(partitionResult.getPartitionId()) == parts.get(index)) {
                        StreamPartition partition = new StreamPartition(entry.getKey(), parts.get(index));

                        String offsetRange = partitionResult.getSequenceNumberRange();
                        if (offsetRange == null) {
                            log.error("partition " + partition + " has been expired and is not readable");
                            throw new PartitionExpiredException("partition " + partition + " has been expired and is not readable");
                        }
                        String[] array = offsetRange.trim().substring(1, offsetRange.length() - 1).split(":");
                        long startOffset = -1;
                        long endOffset = -1;

                        if (!(array[0] == null || array[0].isEmpty() || array[0].contains("null"))) {
                            startOffset = Long.valueOf(array[0].trim());
                            endOffset = Long.valueOf(array[1].trim());
                        }
                        if (startOffset == -1 || endOffset == -1) {
                            throw new IllegalStateException("cannot find offset for " + partition);
                        }
                        results.put(partition, beginningOrEnd ? startOffset : endOffset);
                        index++;
                        if (index >= parts.size()) {
                            break;
                        }
                    }
                }
                if (describeStreamResult.getPartitions().size() < LIMIT) {
                    break;
                }
            }
            if (index < parts.size()) {
                String notExist = "";
                for (; index < parts.size(); index++) {
                    notExist += parts.get(index) + ",";
                }
                new IllegalStateException("some partitions do not exist, topic: "
                        + entry.getKey() + " partition " + notExist);
            }
        }
        return results;
    }

    @Override
    public Map<StreamPartition, Long> beginningOffsets(Collection<StreamPartition> collection) {
        return offsets(collection,true);
    }

    @Override
    public Map<StreamPartition, Long> endOffsets(Collection<StreamPartition> collection) {
        return offsets(collection,false);
    }

    @Override
    protected int getThreadPoolSize() {
        return config.getInt(DisConsumerConfig.MAX_FETCH_THREADS_CONFIG, 50);
    }

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     *
     * @param credentials new credentials
     */
    @Override
    public void updateCredentials(DISCredentials credentials) {
        super.updateCredentials(credentials);
        coordinator.updateInnerClientCredentials(credentials);
    }
}
