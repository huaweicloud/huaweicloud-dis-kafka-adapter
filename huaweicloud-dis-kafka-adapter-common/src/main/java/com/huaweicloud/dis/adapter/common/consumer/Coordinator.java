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

import com.huaweicloud.dis.Constants;
import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.model.ClientState;
import com.huaweicloud.dis.adapter.common.model.DisOffsetAndMetadata;
import com.huaweicloud.dis.adapter.common.model.DisOffsetResetStrategy;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.iface.coordinator.request.HeartbeatRequest;
import com.huaweicloud.dis.iface.coordinator.request.JoinGroupRequest;
import com.huaweicloud.dis.iface.coordinator.request.LeaveGroupRequest;
import com.huaweicloud.dis.iface.coordinator.request.SyncGroupRequest;
import com.huaweicloud.dis.iface.coordinator.response.HeartbeatResponse;
import com.huaweicloud.dis.iface.coordinator.response.JoinGroupResponse;
import com.huaweicloud.dis.iface.coordinator.response.SyncGroupResponse;
import com.huaweicloud.dis.iface.data.request.CommitCheckpointRequest;
import com.huaweicloud.dis.iface.data.request.GetCheckpointRequest;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.response.CommitCheckpointResult;
import com.huaweicloud.dis.iface.data.response.GetCheckpointResult;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;
import com.huaweicloud.dis.iface.stream.response.PartitionResult;
import com.huaweicloud.dis.util.CheckpointTypeEnum;
import com.huaweicloud.dis.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.sleep;


public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private static final long DEFAULT_GENERATION = -1L;

    private InnerDisClient innerDISClient;

    private DISAsync disAsync;

    private ClientState state;

    private String clintId;

    private String groupId;

    private AtomicLong generation;

    private boolean autoCommitEnabled;

    private Map<String, List<Integer>> assignment;

    private ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators;

    private SubscriptionState subscriptions;

    private DelayQueue<DelayedTask> delayedTasks;

    private ArrayBlockingQueue<CommitOffsetThunk> asyncCommitOffsetQueue = new ArrayBlockingQueue<>(1000);

    public volatile boolean isAsyncCommitting = false;

    /**
     * 异步offset提交线程
     */
    private ExecutorService asyncCommitOffsetExecutor;

    public Coordinator(DISAsync disAsync,
                       String clientId,
                       String groupId,
                       SubscriptionState subscriptions,
                       Boolean autoCommitEnabled,
                       long autoCommitIntervalMs,
                       ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators,
                       DISConfig disConfig) {
        this.disAsync = disAsync;
        this.innerDISClient = new InnerDisClient(disConfig);
        this.state = ClientState.INIT;
        this.clintId = clientId;
        this.groupId = groupId;
        this.generation = new AtomicLong(DEFAULT_GENERATION);
        this.autoCommitEnabled = autoCommitEnabled;
        this.subscriptions = subscriptions;
        this.nextIterators = nextIterators;
        this.asyncCommitOffsetExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName("async-commit-task");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.asyncCommitOffsetExecutor.submit(new AsyncCommitOffsetThread());
        this.delayedTasks = new DelayQueue<>();
        this.delayedTasks.add(new AutoCreateAppTask(0));
        this.delayedTasks.add(new HeartbeatDelayedRequest(0L));
        if (this.autoCommitEnabled) {
            this.delayedTasks.add(new AutoCommitTask(System.currentTimeMillis() + autoCommitIntervalMs, autoCommitIntervalMs));
        }
    }

    public boolean isStable() {
        return state == ClientState.STABLE;
    }


    private class HeartbeatDelayedRequest extends DelayedTask {
        private final long nextHeartbeatTimeMs = 10000L;
        private final boolean isDelayedTask;

        public HeartbeatDelayedRequest(long startTimeMs) {
            this(startTimeMs, true);
        }

        public HeartbeatDelayedRequest(long startTimeMs, boolean isDelayedTask) {
            super(startTimeMs);
            this.isDelayedTask = isDelayedTask;
        }

        @Override
        public void doRequest() {
            if (!subscriptions.partitionsAutoAssigned()) {
                // no need heartbeat
                return;
            }

            if (StringUtils.isNullOrEmpty(groupId)) {
                log.error("groupId not defined");
                throw new IllegalArgumentException("groupId not defined");
            }

            if(isDelayedTask) {
                setStartTimeMs(System.currentTimeMillis() + nextHeartbeatTimeMs);
                delayedTasks.add(this);
            }

            HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
            heartbeatRequest.setClientId(clintId);
            heartbeatRequest.setGroupId(groupId);
            heartbeatRequest.setGeneration(generation.get());
            HeartbeatResponse heartbeatResponse = innerDISClient.handleHeartbeatRequest(heartbeatRequest);
            if(state != ClientState.INIT && heartbeatResponse.getState() != HeartbeatResponse.HeartBeatResponseState.STABLE)
            {
                log.info("Heartbeat response [{}] is abnormal", heartbeatResponse.getState());
            }
            //start rebalance
            if (state == ClientState.STABLE && heartbeatResponse.getState() != HeartbeatResponse.HeartBeatResponseState.STABLE) {
                subscriptions.listener().onPartitionsRevoked(new HashSet<>(subscriptions.assignedPartitions()));
                subscriptions.needReassignment();
                log.info("Start to rejoin group [{}] because of status changes to [{}]", groupId, heartbeatResponse.getState());
            }
            //assignment completed
            if (state == ClientState.SYNCING && heartbeatResponse.getState() == HeartbeatResponse.HeartBeatResponseState.STABLE) {
                updateSubscriptions(assignment);
                subscriptions.listener().onPartitionsAssigned(subscriptions.assignedPartitions());
                log.info("Client [{}] success to join group [{}], subscription {}", clintId, groupId, assignment);
            }
            switch (heartbeatResponse.getState()) {
                case JOINING:
                    doJoinGroup();
                    break;
                case SYNCING:
                    doSyncGroup();
                    break;
                case STABLE:
                    if (state == ClientState.SYNCING || state == ClientState.STABLE) {
                        state = ClientState.STABLE;
                    } else {
                        doJoinGroup();
                    }
                    break;
                case GROUP_NOT_EXIST:
                    log.error("Group [{}] not exist", groupId);
                    throw new IllegalArgumentException("Group [" + groupId + "] not exist");
                default:
                    break;
            }
        }
    }

    private class AutoCommitTask extends DelayedTask {
        private long nextCommitTimeMs = 10000L; //10s

        private Map<StreamPartition, DisOffsetAndMetadata> lastCommittedOffsets = null;

        public AutoCommitTask(long startTimeMs, long nextCommitTimeMs) {
            super(startTimeMs);
            this.nextCommitTimeMs = nextCommitTimeMs;
        }

        private void reschedule() {
            setStartTimeMs(System.currentTimeMillis() + nextCommitTimeMs);
            delayedTasks.add(this);
        }

        @Override
        void doRequest() {
            if (!subscriptions.assignedPartitions().isEmpty()) {
                Map<StreamPartition, DisOffsetAndMetadata> newOffsets = subscriptions.allConsumed();
                // no need to commit if no new offsets
                if (newOffsets.size() > 0 && !newOffsets.equals(lastCommittedOffsets)) {
                    try {
                        asyncCommitOffsetQueue.put(new CommitOffsetThunk(subscriptions.allConsumed(), new DisOffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<StreamPartition, DisOffsetAndMetadata> offsets, Exception e) {
                                if (e != null) {
                                    log.error("Failed to commit offsets automatically, offsets [{}], errorInfo [{}]", offsets, e.getMessage(), e);
                                } else {
                                    log.debug("Success to commit offsets automatically, offsets [{}]", offsets);
                                    lastCommittedOffsets = offsets;
                                }
                                reschedule();
                            }
                        }));
                    } catch (InterruptedException e) {
                        reschedule();
                        log.error(e.getMessage(), e);
                    }
                } else {
                    reschedule();
                    log.debug("No new offsets need to be committed, last committed : [{}]", newOffsets);
                }
            } else {
                reschedule();
            }
        }
    }

    /**
     * Create app if not exist
     */
    private class AutoCreateAppTask extends DelayedTask {

        public AutoCreateAppTask(long startTimeMs) {
            super(startTimeMs);
        }

        @Override
        void doRequest() {
            if (!StringUtils.isNullOrEmpty(groupId)) {
                try {
                    disAsync.describeApp(groupId);
                } catch (DISClientException describeException) {
                    if (describeException.getMessage() == null
                            || !describeException.getMessage().contains(Constants.ERROR_CODE_APP_NAME_NOT_EXISTS)) {
                        throw describeException;
                    }
                    try {
                        // app not exist, create
                        disAsync.createApp(groupId);
                        log.info("App [{}] does not exist and created successfully.", groupId);
                    } catch (DISClientException createException) {
                        if (createException.getMessage() == null
                                || !createException.getMessage().contains(Constants.ERROR_CODE_APP_NAME_EXISTS)) {
                            log.error("App {} does not exist and created unsuccessfully. {}", groupId, createException.getMessage());
                            throw createException;
                        }
                    }
                }
            }
        }
    }

    public void doJoinGroup() {
        if (StringUtils.isNullOrEmpty(groupId)) {
            throw new IllegalStateException("groupId not defined, can not send joinGroup request");
        }
        state = ClientState.JOINING;
        JoinGroupRequest joinGroupRequest = new JoinGroupRequest();
        joinGroupRequest.setClientId(clintId);
        joinGroupRequest.setGroupId(groupId);
        if (subscriptions.hasPatternSubscription()) {
            joinGroupRequest.setStreamPattern(subscriptions.getSubscribedPattern().pattern());
        } else {
            List<String> interestedStreams = new ArrayList<>();
            for (String item : subscriptions.subscription()) {
                interestedStreams.add(item);
            }
            joinGroupRequest.setInterestedStream(interestedStreams);
        }
        log.info("[JOIN] Request to join group [{}]", JsonUtils.objToJson(joinGroupRequest));
        JoinGroupResponse joinGroupResponse = innerDISClient.handleJoinGroupRequest(joinGroupRequest);
        switch (joinGroupResponse.getState()) {
            case OK:
                if (subscriptions.hasPatternSubscription()) {
                    subscriptions.changeSubscription(joinGroupResponse.getSubscription());
                }
                log.info("[JOIN] Waiting [{}ms] for other clients to join group [{}]", joinGroupResponse.getSyncDelayedTimeMs(), groupId);
                try {
                    if (joinGroupResponse.getSyncDelayedTimeMs() > 0) {
                        sleep(joinGroupResponse.getSyncDelayedTimeMs());
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
                subscriptions.needRefreshCommits();
                doSyncGroup();
                break;
            case REJOIN:
                log.info("[JOIN] Rejoin group [{}]", groupId);
                doJoinGroup();
                break;
            case ERR_SUBSCRIPTION:
                log.error("[JOIN] Failed to join group, stream {} may be not exist", joinGroupRequest.getInterestedStream());
                throw new IllegalArgumentException("Failed to join group, stream ["
                        + joinGroupRequest.getInterestedStream() + "] may be not exist");
            case GROUP_NOT_EXIST:
                log.error("[JOIN] Failed to join group, group [{}] not exist", groupId);
                throw new IllegalArgumentException("Failed to join group, group [" + groupId + "] not exist");
            case ERROR:
                log.error("[JOIN] Failed to join group, joinGroupResponse error [{}]", JsonUtils.objToJson(joinGroupResponse));
                throw new IllegalStateException("Failed to join group, joinGroupResponse error");
            default:
                log.error("[JOIN] Failed to join group, unknown response [{}]", JsonUtils.objToJson(joinGroupResponse));
                throw new IllegalStateException("Failed to join group, unknown response");
        }
    }

    private void doSyncGroup() {
        if (StringUtils.isNullOrEmpty(groupId)) {
            throw new IllegalStateException("groupId not defined, can not send syncGroup request");
        }
        state = ClientState.SYNCING;
        SyncGroupRequest syncGroupRequest = new SyncGroupRequest();
        syncGroupRequest.setClientId(clintId);
        syncGroupRequest.setGroupId(groupId);
        syncGroupRequest.setGeneration(generation.get());
        log.info("[SYNC] Request to sync group [{}]", JsonUtils.objToJson(syncGroupRequest));
        SyncGroupResponse syncGroupResponse = innerDISClient.handleSyncGroupRequest(syncGroupRequest);
        switch (syncGroupResponse.getState()) {
            case OK:
                log.info("[SYNC] Response for sync group [{}]", JsonUtils.objToJson(syncGroupResponse));
                assignment = syncGroupResponse.getAssignment();
                if (assignment == null) {
                    assignment = Collections.emptyMap();
                }
                this.generation.set(syncGroupResponse.getGeneration());
                return;
            case WAITING:
                log.info("[SYNC] ReSync group [{}]", groupId);
                try {
                    sleep(5000L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
                doSyncGroup();
                break;
            case REJOIN:
                log.info("[SYNC] Rejoin group [{}]", groupId);
                doJoinGroup();
                break;
            case GROUP_NOT_EXIST:
                log.error("[SYNC] Failed to sync group, group [{}] not exist", groupId);
                throw new IllegalArgumentException("Failed to sync group, group [" + groupId + "] not exist");
            case ERROR:
                log.error("[SYNC] Failed to sync group, syncGroupResponse error [{}]", JsonUtils.objToJson(syncGroupResponse));
                throw new IllegalStateException("Failed to sync group, syncGroupResponse error");
            default:
                log.error("[SYNC] Failed to sync group, unknown response [{}]", JsonUtils.objToJson(syncGroupResponse));
                throw new IllegalStateException("Failed to sync group, unknown response");
        }
    }

    public void updateSubscriptions(Map<String, List<Integer>> assignment) {
        List<StreamPartition> partitionsAssignment = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : assignment.entrySet()) {
            for (Integer partition : entry.getValue()) {
                partitionsAssignment.add(new StreamPartition(entry.getKey(), partition));
            }
        }
        Iterator<Map.Entry<StreamPartition, PartitionCursor>> iter = nextIterators.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<StreamPartition, PartitionCursor> entry = iter.next();
            if (!partitionsAssignment.contains(entry.getKey())) {
                iter.remove();
            }
        }
        subscriptions.assignFromSubscribed(partitionsAssignment);
    }

    public void commitSync(Map<StreamPartition, DisOffsetAndMetadata> offsets) {
        commit(Collections.singletonList(new CommitOffsetThunk(offsets, null)));
    }

    public void commit(List<CommitOffsetThunk> asyncCommitOffsetObjList) {
        if (asyncCommitOffsetObjList.size() == 0) {
            return;
        }

        if (StringUtils.isNullOrEmpty(this.groupId)) {
            throw new IllegalStateException("groupId not defined, checkpoint not commit");
        }

        Map<StreamPartition, DisOffsetAndMetadata> offsets = new HashMap<>();
        for (CommitOffsetThunk asyncCommitOffsetObj : asyncCommitOffsetObjList) {
            offsets.putAll(asyncCommitOffsetObj.offsets);
        }

        if (offsets.size() == 0) {
            return;
        }

        for (Map.Entry<StreamPartition, DisOffsetAndMetadata> offset : offsets.entrySet()) {
            if (!subscriptions.isAssigned(offset.getKey())) {
                throw new IllegalStateException(offset.getKey().toString() + " is not assigned!");
            }
        }

        final CountDownLatch countDownLatch = new CountDownLatch(offsets.size());
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        final AtomicBoolean lock = new AtomicBoolean(false);
        for (Map.Entry<StreamPartition, DisOffsetAndMetadata> offset : offsets.entrySet()) {
            CommitCheckpointRequest commitCheckpointRequest = new CommitCheckpointRequest();
            commitCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
            commitCheckpointRequest.setSequenceNumber(String.valueOf(offset.getValue().offset()));
            commitCheckpointRequest.setAppName(groupId);
            commitCheckpointRequest.setMetadata(offset.getValue().metadata());
            commitCheckpointRequest.setStreamName(offset.getKey().stream());
            commitCheckpointRequest.setPartitionId(Utils.getShardIdStringFromPartitionId(offset.getKey().partition()));
            disAsync.commitCheckpointAsync(commitCheckpointRequest, new AsyncHandler<CommitCheckpointResult>() {
                @Override
                public void onError(Exception exception) {
                    exceptionAtomicReference.set(exception);
                    countDownLatch.countDown();
                    log.error("Failed to commitCheckpointAsync, error : [{}], request : [{}]",
                            exception.getMessage(), JsonUtils.objToJson(commitCheckpointRequest));

                    try {
                        countDownLatch.await();
                    } catch (InterruptedException ignored) {
                    }

                    if (lock.compareAndSet(false, true)) {
                        for (CommitOffsetThunk asyncCommitOffsetObj : asyncCommitOffsetObjList) {
                            if (asyncCommitOffsetObj.callback != null) {
                                asyncCommitOffsetObj.callback.onComplete(asyncCommitOffsetObj.offsets, exceptionAtomicReference.get());
                            }
                        }
                    }
                }

                @Override
                public void onSuccess(CommitCheckpointResult commitCheckpointResult) {
                    if (subscriptions.isAssigned(offset.getKey())) {
                        subscriptions.committed(offset.getKey(), offset.getValue());
                    }
                    countDownLatch.countDown();
                    log.debug("Success to commitCheckpointAsync, {} : {}", offset.getKey(), offset.getValue());
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException ignored) {
                    }

                    if (exceptionAtomicReference.get() == null) {
                        if (lock.compareAndSet(false, true)) {
                            for (CommitOffsetThunk asyncCommitOffsetObj : asyncCommitOffsetObjList) {
                                if (asyncCommitOffsetObj.callback != null) {
                                    asyncCommitOffsetObj.callback.onComplete(asyncCommitOffsetObj.offsets, null);
                                }
                            }
                        }
                    }
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        handleError(exceptionAtomicReference);
    }

    public void commitAsync(Map<StreamPartition, DisOffsetAndMetadata> offsets, DisOffsetCommitCallback callback) {
        try {
            asyncCommitOffsetQueue.put(new CommitOffsetThunk(offsets, callback));
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void executeDelayedTask() {
        DelayedTask delayedTask = null;
        try {
            while ((delayedTask = delayedTasks.poll(0, TimeUnit.MILLISECONDS)) != null) {
                delayedTask.doRequest();
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void addPartition(Map<String, List<Integer>> describeTopic, StreamPartition part) {
        if (describeTopic.get(part.stream()) == null) {
            describeTopic.putIfAbsent(part.stream(), new ArrayList<>());
        }
        describeTopic.get(part.stream()).add(part.partition());
    }

    public void updateFetchPositions(Set<StreamPartition> partitions) {
        // refresh commits for all assigned partitions
        refreshCommittedOffsetsIfNeeded();

        Map<String, List<Integer>> describeTopic = new HashMap<>();
        // then do any offset lookups in case some positions are not known
        // reset the fetch position to the committed position
        for (StreamPartition tp : partitions) {
            if (!subscriptions.isAssigned(tp) || subscriptions.isFetchable(tp))
                continue;

            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if (subscriptions.isOffsetResetNeeded(tp)) {
                addPartition(describeTopic, tp);
                //            resetOffset(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                subscriptions.needOffsetReset(tp);

                //            resetOffset(tp);
                addPartition(describeTopic, tp);
            } else {
                long committed = subscriptions.committed(tp).offset();
                log.debug("Resetting offset for partition {} to the committed offset {}", tp, committed);
                subscriptions.seek(tp, committed);
            }
        }
        resetOffsetByTopic(describeTopic);
    }

    private void resetOffsetByTopic(Map<String, List<Integer>> describeTopic) {
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
                String partitionId = parts.get(index) == 0 ? "" : Utils.getShardIdStringFromPartitionId(parts.get(index) - 1);
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(entry.getKey());
                describeStreamRequest.setLimitPartitions(100);
                describeStreamRequest.setStartPartitionId(partitionId);
                DescribeStreamResult describeStreamResult = disAsync.describeStream(describeStreamRequest);
                for (PartitionResult partitionResult : describeStreamResult.getPartitions()) {
                    if (Utils.getKafkaPartitionFromPartitionId(partitionResult.getPartitionId()) == parts.get(index)) {
                        StreamPartition partition = new StreamPartition(entry.getKey(), parts.get(index));
                        DisOffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
                        if (strategy != DisOffsetResetStrategy.EARLIEST && strategy != DisOffsetResetStrategy.LATEST) {
                            throw new NoOffsetForPartitionException(partition);
                        }

                        String offsetRange = partitionResult.getSequenceNumberRange();
                        long offset = -1;
                        if (offsetRange == null) {
                            log.error("partition " + partition + " has been expired and is not readable");
                            throw new PartitionExpiredException("partition " + partition + " has been expired and is not readable");
                        }
                        String[] array = offsetRange.trim().substring(1, offsetRange.length() - 1).split(":");
                        long startOffset = 0;
                        long endOffset = 0;

                        if (!(array[0] == null || array[0].isEmpty() || array[0].contains("null"))) {
                            startOffset = Long.valueOf(array[0].trim());
                            endOffset = Long.valueOf(array[1].trim());
                        }
                        if (strategy == DisOffsetResetStrategy.EARLIEST) {
                            offset = startOffset;
                        } else if (strategy == DisOffsetResetStrategy.LATEST) {
                            offset = endOffset;
                        }
                        if (offset == -1) {
                            return;
                        }

                        log.debug("Resetting offset for partition {} to {} offset, strategy {}.",
                                partition,
                                offset,
                                strategy.name().toLowerCase(Locale.ROOT));

                        // we might lose the assignment while fetching the offset, so check it is still active
                        if (subscriptions.isAssigned(partition)) {
                            this.subscriptions.seek(partition, offset);
                            nextIterators.remove(partition);
                        }
                        index++;
                        if (index >= parts.size()) {
                            break;
                        }
                    }
                }
                if (describeStreamResult.getPartitions().size() < 100) {
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
    }

    private void resetOffset(StreamPartition partition) {
        DisOffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        if (strategy != DisOffsetResetStrategy.EARLIEST && strategy != DisOffsetResetStrategy.LATEST) {
            throw new NoOffsetForPartitionException(partition);
        }
        String partitionId = partition.partition() == 0 ? "" : Utils.getShardIdStringFromPartitionId(partition.partition() - 1);
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(partition.stream());
        describeStreamRequest.setLimitPartitions(1);
        describeStreamRequest.setStartPartitionId(partitionId);
        DescribeStreamResult describeStreamResult = disAsync.describeStream(describeStreamRequest);
        String offsetRange = describeStreamResult.getPartitions().get(0).getSequenceNumberRange();
        long offset = -1;
        if (offsetRange == null) {
            log.error("partition " + partition + " has been expired and is not readable");
            throw new PartitionExpiredException("partition " + partition + " has been expired and is not readable");
        }
        String[] array = offsetRange.trim().substring(1, offsetRange.length() - 1).split(":");
        long startOffset = 0;
        long endOffset = 0;

        if (!(array[0] == null || array[0].isEmpty() || array[0].contains("null"))) {
            startOffset = Long.valueOf(array[0].trim());
            endOffset = Long.valueOf(array[1].trim());
        }
        if (strategy == DisOffsetResetStrategy.EARLIEST) {
            offset = startOffset;
        } else if (strategy == DisOffsetResetStrategy.LATEST) {
            offset = endOffset;
        }
        if (offset == -1) {
            return;
        }

        log.debug("Resetting offset for partition {} to {} offset, strategy {}", partition, offset, strategy.name().toLowerCase(Locale.ROOT));

        // we might lose the assignment while fetching the offset, so check it is still active
        if (subscriptions.isAssigned(partition)) {
            this.subscriptions.seek(partition, offset);
            nextIterators.remove(partition);
        }

    }

    public void refreshCommittedOffsetsIfNeeded() {
        if (StringUtils.isNullOrEmpty(this.groupId)) {
            log.debug("groupId is null, can not fetch checkpoint.");
            return;
        }
        if (subscriptions.refreshCommitsNeeded()) {
            Map<StreamPartition, DisOffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            for (Map.Entry<StreamPartition, DisOffsetAndMetadata> entry : offsets.entrySet()) {
                StreamPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp))
                    this.subscriptions.committed(tp, entry.getValue());
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    public Map<StreamPartition, DisOffsetAndMetadata> fetchCommittedOffsets(Set<StreamPartition> partitions) {
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        final CountDownLatch countDownLatch = new CountDownLatch(partitions.size());
        final Map<StreamPartition, DisOffsetAndMetadata> offsets = new ConcurrentHashMap<>();
        for (StreamPartition partition : partitions) {
            GetCheckpointRequest getCheckpointRequest = new GetCheckpointRequest();
            getCheckpointRequest.setStreamName(partition.stream());
            getCheckpointRequest.setAppName(groupId);
            getCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
            getCheckpointRequest.setPartitionId(Utils.getShardIdStringFromPartitionId(partition.partition()));
            disAsync.getCheckpointAsync(getCheckpointRequest, new AsyncHandler<GetCheckpointResult>() {
                @Override
                public void onError(Exception e) {
                    exceptionAtomicReference.set(e);
                    countDownLatch.countDown();
                    log.error("Failed to getCheckpointAsync, error : [{}], request : [{}]",
                            e.getMessage(), JsonUtils.objToJson(getCheckpointRequest));
                }

                @Override
                public void onSuccess(GetCheckpointResult getCheckpointResult) {
                    try {
                        if (getCheckpointResult.getMetadata() == null) {
                            getCheckpointResult.setMetadata("");
                        }

                        if (!StringUtils.isNullOrEmpty(getCheckpointResult.getSequenceNumber())
                                && Long.valueOf(getCheckpointResult.getSequenceNumber()) >= 0) {
                            offsets.put(partition,
                                    new DisOffsetAndMetadata(Long.valueOf(getCheckpointResult.getSequenceNumber()),
                                            getCheckpointResult.getMetadata()));
                        }
                        log.debug("{} get checkpoint {} ", partition, getCheckpointResult.getSequenceNumber());
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

        handleError(exceptionAtomicReference);

        return offsets;
    }


    public void maybeLeaveGroup() {
        if (generation.get() >= 0L) {
            generation.set(DEFAULT_GENERATION);
            LeaveGroupRequest leaveGroupRequest = new LeaveGroupRequest();
            leaveGroupRequest.setClientId(clintId);
            leaveGroupRequest.setGroupId(groupId);
            innerDISClient.handleLeaveGroupRequest(leaveGroupRequest);
        }
    }

    public void seek(final Set<StreamPartition> streamPartitions) {
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        final CountDownLatch countDownLatch = new CountDownLatch(streamPartitions.size());
        for (StreamPartition partition : streamPartitions) {
            final String startingSequenceNumber = String.valueOf(subscriptions.position(partition));
            final GetPartitionCursorRequest getShardIteratorParam = new GetPartitionCursorRequest();
            getShardIteratorParam.setPartitionId(Utils.getShardIdStringFromPartitionId(partition.partition()));
            getShardIteratorParam.setStartingSequenceNumber(startingSequenceNumber);
            getShardIteratorParam.setStreamName(partition.stream());

            disAsync.getPartitionCursorAsync(getShardIteratorParam, new AsyncHandler<GetPartitionCursorResult>() {
                @Override
                public void onError(Exception exception) {
                    // handle "Sequence_number out of range"
                    if (exception.getMessage().contains(Constants.ERROR_CODE_SEQUENCE_NUMBER_OUT_OF_RANGE)) {
                        try {
                            int start = exception.getMessage().indexOf(Constants.ERROR_INFO_SEQUENCE_NUMBER_OUT_OF_RANGE);
                            final String message;
                            if (start > -1) {
                                message = exception.getMessage().substring(start, exception.getMessage().length() - 2);
                            } else {
                                message = exception.getMessage();
                            }
                            // special treat for out of range partition
                            subscriptions.needOffsetReset(partition);
                            resetOffset(partition);
                            String newStartingSequenceNumber = String.valueOf(subscriptions.position(partition));
                            getShardIteratorParam.setStartingSequenceNumber(newStartingSequenceNumber);
                            disAsync.getPartitionCursorAsync(getShardIteratorParam, new AsyncHandler<GetPartitionCursorResult>() {
                                @Override
                                public void onError(Exception newException) {
                                    exceptionReference.set(newException);
                                    countDownLatch.countDown();
                                    log.error("Failed to getPartitionCursorAsync, error : [{}], request : [{}]",
                                            newException.getMessage(), JsonUtils.objToJson(getShardIteratorParam));
                                }

                                @Override
                                public void onSuccess(GetPartitionCursorResult getPartitionCursorResult) {
                                    if (!StringUtils.isNullOrEmpty(getPartitionCursorResult.getPartitionCursor())) {
                                        nextIterators.put(partition, new PartitionCursor(getPartitionCursorResult.getPartitionCursor()));
                                    } else {
                                        requestRebalance();
                                    }
                                    countDownLatch.countDown();
                                    log.warn("{} {}, will starts from sequenceNumber {}.", partition, message, newStartingSequenceNumber);
                                }
                            });
                        } catch (Exception newException) {
                            exceptionReference.set(newException);
                            countDownLatch.countDown();
                            log.error("Failed to resetOffset, error : [{}], request : [{}]",
                                    newException.getMessage(), JsonUtils.objToJson(getShardIteratorParam));
                        }
                    } else {
                        exceptionReference.set(exception);
                        countDownLatch.countDown();
                        log.error("Failed to getPartitionCursorAsync, error : [{}], request : [{}]",
                                exception.getMessage(), JsonUtils.objToJson(getShardIteratorParam));
                    }
                }

                @Override
                public void onSuccess(GetPartitionCursorResult getPartitionCursorResult) {
                    nextIterators.put(partition, new PartitionCursor(getPartitionCursorResult.getPartitionCursor()));
                    countDownLatch.countDown();
                    log.debug("{} will starts from sequenceNumber {}.", partition, startingSequenceNumber);
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        handleError(exceptionReference);
    }

    public boolean ensureGroupStable() {
        if (isStable()) {
            return true;
        } else {
            int loop = 0;
            while (state != ClientState.STABLE) {
                // start right now
                new HeartbeatDelayedRequest(0, false).doRequest();
                if (++loop > 50) {
                    try {
                        sleep(50);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
        return true;
    }

    public void requestRebalance() {
        generation.set(DEFAULT_GENERATION);
    }

    private void handleError(AtomicReference<Exception> exceptionAtomicReference) {
        if (exceptionAtomicReference != null) {
            Exception e = exceptionAtomicReference.get();
            if (e != null) {
                if (e instanceof DISClientException) {
                    throw (DISClientException) e;
                } else {
                    throw new DISClientException(e);
                }
            }
        }
    }

    /**
     * Update DIS credentials, such as ak/sk/securityToken
     *
     * @param credentials new credentials
     */
    public void updateInnerClientCredentials(DISCredentials credentials) {
        innerDISClient.updateCredentials(credentials);
    }

    private class CommitOffsetThunk {
        private Map<StreamPartition, DisOffsetAndMetadata> offsets;
        private DisOffsetCommitCallback callback;

        public CommitOffsetThunk(Map<StreamPartition, DisOffsetAndMetadata> offsets, DisOffsetCommitCallback callback) {
            if (offsets == null) {
                throw new IllegalArgumentException("offsets can not be null");
            }
            this.offsets = offsets;
            this.callback = callback;
        }
    }

    private class AsyncCommitOffsetThread implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // async commit should be in queue
                    CommitOffsetThunk commitOffsetThunk = asyncCommitOffsetQueue.take();
                    isAsyncCommitting = true;
                    List<CommitOffsetThunk> commitOffsetThunkList = new LinkedList<>();
                    commitOffsetThunkList.add(commitOffsetThunk);
                    // get all
                    asyncCommitOffsetQueue.drainTo(commitOffsetThunkList);
                    int remainingSize = asyncCommitOffsetQueue.size();
                    if (remainingSize > 0) {
                        log.debug("The number of offsets waiting to be submitted in the queue is {}.", remainingSize);
                    }
                    try {
                        commit(commitOffsetThunkList);
                    } finally {
                        isAsyncCommitting = false;
                    }
                } catch (DISClientException | InterruptedException ignored) {
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public void close() {
        long start = System.currentTimeMillis();
        int loop = 0;
        while (loop < 3) {
            if ((System.currentTimeMillis() - start) >= 5000) {
                break;
            }
            if (isAsyncCommitting || asyncCommitOffsetQueue.size() > 0) {
                loop = 0;
            } else {
                loop++;
            }
            Utils.sleep(5);
        }
        if (asyncCommitOffsetExecutor != null) {
            asyncCommitOffsetExecutor.shutdownNow();
        }
    }
}
