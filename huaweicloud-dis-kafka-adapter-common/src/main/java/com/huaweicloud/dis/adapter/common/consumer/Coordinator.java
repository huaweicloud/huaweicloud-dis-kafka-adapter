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
import com.huaweicloud.dis.adapter.common.utils.DISThread;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.exception.*;
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

import java.util.*;
import java.util.concurrent.*;
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

    private long autoCommitIntervalMs;

    private boolean periodicHeartbeatEnabled;

    private long heartbeatIntervalMs;

    private long rebalanceTimeoutMs;

    private boolean accelerateAssignEnabled;

    private Map<String, List<Integer>> assignment;

    private ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators;

    private SubscriptionState subscriptions;

    private DelayQueue<DelayedTask> delayedTasks;

    private ArrayBlockingQueue<CommitOffsetThunk> asyncCommitOffsetQueue = new ArrayBlockingQueue<>(1000);

    public volatile boolean isAsyncCommitting = false;

    private Map<String, Integer> oldStreamReadablePartitionCountMap = new ConcurrentHashMap<>();

    private Map<String, Integer> curStreamReadablePartitionCountMap = new ConcurrentHashMap<>();

    private Map<StreamPartition, Long> lastCommitOffsetMap = new ConcurrentHashMap<>();

    boolean enableSubscribeExpandingAdapter = true;
    /**
     * 异步offset提交线程
     */
    private AsyncCommitOffsetThread asyncCommitOffsetThread;

    /**
     * 定时心跳线程
     */
    private PeriodicHeartbeatThread heartbeatThread;

    public Coordinator(DISAsync disAsync,
                       String clientId,
                       String groupId,
                       SubscriptionState subscriptions,
                       Boolean autoCommitEnabled,
                       long autoCommitIntervalMs,
                       Boolean periodicHeartbeatEnabled,
                       long heartbeatIntervalMs,
                       long rebalanceTimeoutMs,
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
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        /*this.asyncCommitOffsetExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName("async-commit-task");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.asyncCommitOffsetExecutor.submit(new AsyncCommitOffsetThread());*/
        asyncCommitOffsetThread = new AsyncCommitOffsetThread();
        asyncCommitOffsetThread.start();
        this.delayedTasks = new DelayQueue<>();
        this.delayedTasks.add(new AutoCreateAppTask(0));
        this.delayedTasks.add(new HeartbeatDelayedRequest(0L));
        if (this.autoCommitEnabled) {
            this.delayedTasks.add(new AutoCommitTask(System.currentTimeMillis() + autoCommitIntervalMs, autoCommitIntervalMs));
        }
        this.enableSubscribeExpandingAdapter = Boolean.valueOf(disConfig.get("enable.subscribe.expanding.adapter", "true"));

        this.periodicHeartbeatEnabled = periodicHeartbeatEnabled;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;

        this.accelerateAssignEnabled = Boolean.valueOf(disConfig.get(DisConsumerConfig.ENABLE_ACCELERATE_ASSIGN_CONFIG, "false"));
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

            HeartbeatResponse heartbeatResponse;
            if (state == ClientState.STABLE && generation.get() == -1) {
                heartbeatResponse = new HeartbeatResponse();
                heartbeatResponse.setState(HeartbeatResponse.HeartBeatResponseState.JOINING);
            } else {
                HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
                heartbeatRequest.setClientId(clintId);
                heartbeatRequest.setGroupId(groupId);
                heartbeatRequest.setGeneration(generation.get());
                heartbeatResponse = innerDISClient.handleHeartbeatRequest(heartbeatRequest);
            }

            if(state != ClientState.INIT && heartbeatResponse.getState() != HeartbeatResponse.HeartBeatResponseState.STABLE)
            {
                log.warn("Group [{}] heartbeat response [{}] is abnormal.", groupId, heartbeatResponse.getState());
            }
            //start rebalance
            if (state == ClientState.STABLE && heartbeatResponse.getState() != HeartbeatResponse.HeartBeatResponseState.STABLE) {
                subscriptions.listener().onPartitionsRevoked(new HashSet<>(subscriptions.assignedPartitions()));
                subscriptions.needReassignment();
                subscriptions.needRefreshCommits();
                log.info("Start to rejoin group [{}] because of status changes to [{}]", groupId, heartbeatResponse.getState());
            }
            //assignment completed
            if (state == ClientState.SYNCING && heartbeatResponse.getState() == HeartbeatResponse.HeartBeatResponseState.STABLE) {
                getSubscribeStreamPartitionCount();
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
        joinGroupRequest.setAccelerateAssignEnabled(this.accelerateAssignEnabled);
        joinGroupRequest.setRebalanceTimeoutMs(this.rebalanceTimeoutMs);
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
                throw new IllegalArgumentException("Failed to join group, stream "
                        + joinGroupRequest.getInterestedStream() + " may be not exist");
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
                if (this.state != ClientState.STABLE && this.state != ClientState.SYNCING) {
                    this.state = ClientState.SYNCING;
                }
                return;
            case WAITING:
                log.info("[SYNC] ReSync group [{}], clientId [{}]", groupId, clintId);
                try {
                    sleep(500L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
                doSyncGroup();
                break;
            case REJOIN:
                log.info("[SYNC] Rejoin group [{}], clientId [{}]", groupId, clintId);
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
        // 清空以前的提交记录，重新提交
        for (StreamPartition streamPartition : offsets.keySet()) {
            lastCommitOffsetMap.remove(streamPartition);
        }
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
        final Map<StreamPartition, Exception> streamPartitionExceptionMap = new HashMap<>();
        for (Map.Entry<StreamPartition, DisOffsetAndMetadata> offset : offsets.entrySet()) {
            StreamPartition streamPartition = offset.getKey();
            // 获取上一次提交位置
            Long lastCommitted = lastCommitOffsetMap.get(streamPartition);
            if (lastCommitted != null && lastCommitted == offset.getValue().offset()) {
                // 和上一次提交位置相同，则不重复发起请求
                log.debug("{} already commit {}", streamPartition, lastCommitted);
                countDownLatch.countDown();
                continue;
            }
            CommitCheckpointRequest commitCheckpointRequest = new CommitCheckpointRequest();
            commitCheckpointRequest.setCheckpointType(CheckpointTypeEnum.LAST_READ.name());
            commitCheckpointRequest.setSequenceNumber(String.valueOf(offset.getValue().offset()));
            commitCheckpointRequest.setAppName(groupId);
            if (subscriptions.partitionsAutoAssigned()) {
                commitCheckpointRequest.setClientId(clintId);
                commitCheckpointRequest.setGenerationId(generation.get());
            }
            commitCheckpointRequest.setMetadata(offset.getValue().metadata());
            commitCheckpointRequest.setStreamName(streamPartition.stream());
            commitCheckpointRequest.setPartitionId(Utils.getShardIdStringFromPartitionId(streamPartition.partition()));
            disAsync.commitCheckpointAsync(commitCheckpointRequest, new AsyncHandler<CommitCheckpointResult>() {
                @Override
                public void onError(Exception exception) {
                    if (!(exception instanceof DISConsumerMemberNotExistsException)
                            || !(exception instanceof DISConsumerGroupRebalanceInProgressException)
                            || !(exception instanceof DISConsumerGroupIllegalGenerationException)) {
                        // 由于rebalanced导致的checkpoint提交失败，打印日志即可，无需将异常抛出
                        streamPartitionExceptionMap.put(streamPartition, exception);
                    }
                    countDownLatch.countDown();
                    log.error("Failed to commitCheckpointAsync, error : [{}], request : [{}]",
                            exception.getMessage(), JsonUtils.objToJson(commitCheckpointRequest));
                }

                @Override
                public void onSuccess(CommitCheckpointResult commitCheckpointResult) {
                    if (subscriptions.isAssigned(streamPartition)) {
                        subscriptions.committed(streamPartition, offset.getValue());
                    }
                    lastCommitOffsetMap.put(streamPartition, offset.getValue().offset());
                    countDownLatch.countDown();
                    log.debug("Success to commitCheckpointAsync, {} : {}", offset.getKey(), offset.getValue());
                }
            });
        }

        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            exceptionAtomicReference.set(e);
        }

        try {
            // 对每个回调函数进行处理
            for (CommitOffsetThunk asyncCommitOffsetObj : asyncCommitOffsetObjList) {
                if (asyncCommitOffsetObj.callback != null) {
                    Exception exception = exceptionAtomicReference.get();
                    if (exception == null) {
                        // 只要有一个分区提交失败，则回调失败
                        for (StreamPartition streamPartition : asyncCommitOffsetObj.offsets.keySet()) {
                            exception = streamPartitionExceptionMap.get(streamPartition);
                            if (exception != null) {
                                exceptionAtomicReference.set(exception);
                                break;
                            }
                        }
                    }
                    asyncCommitOffsetObj.callback.onComplete(asyncCommitOffsetObj.offsets, exception);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        handleError(exceptionAtomicReference);
    }

    public void commitAsync(Map<StreamPartition, DisOffsetAndMetadata> offsets, DisOffsetCommitCallback callback) {
        try {
            // 由用户触发的请求，清空以前提交记录，重新提交
            for (StreamPartition streamPartition : offsets.keySet()) {
                lastCommitOffsetMap.remove(streamPartition);
            }
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
                if (isExpandPartition(tp)) {
                    // Subscribe模式下扩容分区，则从EARLIEST开始防止数据丢失
                    subscriptions.needOffsetReset(tp, DisOffsetResetStrategy.EARLIEST);
                    log.info("Find new expand partition {}, checkpoint is null, so will starts from EARLIEST.", tp);
                } else {
                    subscriptions.needOffsetReset(tp);
                }
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
                    int id = Utils.getKafkaPartitionFromPartitionId(partitionResult.getPartitionId());
                    if (id == parts.get(index)) {
                        StreamPartition partition = new StreamPartition(entry.getKey(), parts.get(index));
                        DisOffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
                        if (strategy != DisOffsetResetStrategy.EARLIEST && strategy != DisOffsetResetStrategy.LATEST) {
                            throw new NoOffsetForPartitionException(partition);
                        }

                        String offsetRange = partitionResult.getSequenceNumberRange();
                        long offset = -1;
                        if (offsetRange == null) {
                            if (subscriptions.partitionsAutoAssigned()) {
                                log.warn("Partition {} get null sequenceNumberRange.", partition);
                                continue;
                            }
                            log.error("Partition " + partition + " has been expired and is not readable");
                            throw new PartitionExpiredException("partition " + partition + " has been expired and is not readable");
                        }

                        // range格式不对，可能表示此分区不存在或正在扩缩容
                        if (!offsetRange.startsWith("[")) {
                            if (subscriptions.partitionsAutoAssigned()) {
                                log.warn("Partition {} get wrong sequenceNumberRange {}.", partition, offsetRange);
                                continue;
                            }
                            log.error("Failed to get partition {} sequenceNumberRange {}.", partition, offsetRange);
                            throw new DISPartitionNotExistsException("partition " + partition + " not exists");
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
            if (subscriptions.position(partition) == null) {
                countDownLatch.countDown();
                continue;
            }
            // 重设分区位置，清理上一次提交位置
            lastCommitOffsetMap.remove(partition);
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
                            if (isExpandPartition(partition)) {
                                subscriptions.needOffsetReset(partition, DisOffsetResetStrategy.EARLIEST);
                                log.info("Find new expand partition {}, checkpoint is {} but out of range {}, so will starts from EARLIEST.",
                                        partition, startingSequenceNumber, message);
                            } else {
                                subscriptions.needOffsetReset(partition);
                            }
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
            activePeriodicHeartbeat(); // 激活定时心跳
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
            activePeriodicHeartbeat(); // 激活定时心跳
        }
        return true;
    }

    public boolean activePeriodicHeartbeat() {
        if (this.periodicHeartbeatEnabled && this.heartbeatThread == null) {
            log.info("Periodic Heartbeat Task is active now, groupId: {}, clientId: {}.", groupId, clintId);

            heartbeatThread = new PeriodicHeartbeatThread(heartbeatIntervalMs);
            heartbeatThread.start();
        }
        return true;
    }

    public void requestRebalance() {
        generation.set(DEFAULT_GENERATION);
    }

    /**
     * 抛出DISException，用于上层业务捕获adapter的异常
     *
     * @param exceptionAtomicReference 异常
     */
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

    public void updateAuthToken(String authToken){
        innerDISClient.updateAuthToken(authToken);
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

    private class AsyncCommitOffsetThread extends DISThread {

        private boolean closed = false;

        public AsyncCommitOffsetThread() {
            super("async-commit-task", true);
        }

        public void close() {
            this.closed = true;
        }

        @Override
        public void run() {
            while (true) {

                if (closed) {
                    return;
                }

                try {
                    // async commit should be in queue
                    // CommitOffsetThunk commitOffsetThunk = asyncCommitOffsetQueue.take();
                    CommitOffsetThunk commitOffsetThunk = asyncCommitOffsetQueue.poll(autoCommitIntervalMs, TimeUnit.MILLISECONDS);
                    if (commitOffsetThunk == null)
                        continue;
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

    private class PeriodicHeartbeatThread extends DISThread {

        private boolean closed = false;
        private long heartbeatIntervalMs;

        public PeriodicHeartbeatThread(long heartbeatIntervalMs) {
            super("dis-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId), true);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public void close() {
            this.closed = true;
        }

        @Override
        public void run() {
            try {
                while (true) {

                    if (closed) {
                        return;
                    }

                    if (state == ClientState.STABLE || state == ClientState.SYNCING) {

                        try {
                            long begin = System.currentTimeMillis();
                            HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
                            heartbeatRequest.setClientId(clintId);
                            heartbeatRequest.setGroupId(groupId);
                            heartbeatRequest.setGeneration(generation.get());
                            HeartbeatResponse heartbeatResponse = innerDISClient.handleHeartbeatRequest(heartbeatRequest);
                            if (heartbeatResponse.getState() != HeartbeatResponse.HeartBeatResponseState.STABLE) {
                                log.warn("Periodic Heartbeat Thread: Group is not stable, group state: {}, groupId: {}", heartbeatResponse.getState(), groupId);
                                new HeartbeatDelayedRequest(0, false).doRequest();
                            }
                            long end = System.currentTimeMillis();
                            if (end - begin > 30000L) {
                                log.warn("Periodic Heartbeat Thread: heartbeat request took more than 30000MS, spendTime: {}", end - begin);
                            }
                            log.debug("Heartbeat beginTime: {}, endTime: {}, cost: {}, heartbeatInterval: {}, threadid: {}", begin, end, end - begin, heartbeatIntervalMs, Thread.currentThread().getId());
                        } catch (Exception e) {
                            log.error("Failed to invoke heartbeat request, errorInfo [{}]", e.getMessage(), e);
                        }

                        Thread.sleep(heartbeatIntervalMs);
                    }
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error", groupId, e);
            } finally {
                log.debug("Heartbeat thread for group {} has closed", groupId);
            }
        }
    }

    private void closeHeartbeatThread() {
        if (heartbeatThread != null) {
            heartbeatThread.close();

            try {
                heartbeatThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            }
        }
    }

    private void closeAsyncCommitOffsetThread() {
        if (asyncCommitOffsetThread != null) {
            asyncCommitOffsetThread.close();

            try {
                asyncCommitOffsetThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for consumer async commit offset thread to close");
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
        closeAsyncCommitOffsetThread(); // 关闭定时提交 Offset 线程
        closeHeartbeatThread(); // 关闭定时心跳线程
    }

    /**
     * 获取Subscribe模式下通道的可读分区数量
     */
    public void getSubscribeStreamPartitionCount() {
        if (subscriptions.partitionsAutoAssigned() && !subscriptions.hasPatternSubscription() && enableSubscribeExpandingAdapter) {
            oldStreamReadablePartitionCountMap.clear();
            oldStreamReadablePartitionCountMap.putAll(curStreamReadablePartitionCountMap);
            curStreamReadablePartitionCountMap.clear();
            CountDownLatch countDownLatch = new CountDownLatch(subscriptions.subscription().size());
            for (String streamName : subscriptions.subscription()) {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                describeStreamRequest.setLimitPartitions(1);
                disAsync.describeStreamAsync(describeStreamRequest, new AsyncHandler<DescribeStreamResult>() {
                    @Override
                    public void onError(Exception exception) {
                        countDownLatch.countDown();
                        log.warn(exception.getMessage(), exception);
                    }

                    @Override
                    public void onSuccess(DescribeStreamResult describeStreamResult) {
                        countDownLatch.countDown();
                        curStreamReadablePartitionCountMap.put(streamName, describeStreamResult.getReadablePartitionCount());
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                log.warn(e.getMessage(), e);
            }
            log.info("old stream readable partition {}, cur stream readable partition {}",
                    oldStreamReadablePartitionCountMap, curStreamReadablePartitionCountMap);
        }
    }

    boolean isExpandPartition(StreamPartition streamPartition) {
        Integer oldPartitionCount = oldStreamReadablePartitionCountMap.get(streamPartition.stream());
        return oldPartitionCount != null && streamPartition.partition() + 1 > oldPartitionCount;
    }
}
