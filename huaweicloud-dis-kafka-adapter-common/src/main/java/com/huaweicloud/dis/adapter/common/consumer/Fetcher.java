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

import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.common.Utils;
import com.huaweicloud.dis.adapter.common.model.StreamPartition;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.exception.DISPartitionExpiredException;
import com.huaweicloud.dis.exception.DISPartitionNotExistsException;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.iface.data.response.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


public class Fetcher {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private DISConfig disConfig;

    private DISAsync disAsync;

    private ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators;

    private SubscriptionState subscriptions;

    private AtomicInteger receivedCnt;

    private Map<StreamPartition, Future<GetRecordsResult>> futures;

    private Coordinator coordinator;

    private volatile boolean forceWakeup = false;

    private final Map<StreamPartition, Integer> partitionException = new ConcurrentHashMap<>();

    private DISClientException exception = null;

    public Fetcher(DISAsync disAsync, DISConfig disConfig, SubscriptionState subscriptions,
                   Coordinator coordinator, ConcurrentHashMap<StreamPartition, PartitionCursor> nextIterators) {
        this.disConfig = disConfig;
        this.subscriptions = subscriptions;
        this.coordinator = coordinator;
        this.receivedCnt = new AtomicInteger(0);
        this.nextIterators = nextIterators;
        this.futures = new ConcurrentHashMap<>();
        this.disAsync = disAsync;
    }

    public void sendFetchRequests() {
        if (!subscriptions.hasAllFetchPositions()) {
            coordinator.updateFetchPositions(subscriptions.missingFetchPositions());
        }

        Set<StreamPartition> needUpdateOffset = new HashSet<>();
        for (StreamPartition partition : subscriptions.assignedPartitions()) {
            if (!subscriptions.isPaused(partition)) {
                if (nextIterators.get(partition) == null || nextIterators.get(partition).isExpire()) {
                    needUpdateOffset.add(partition);
                    // delete response from future when position reset. but lead to more poll() costs.
                    Future<GetRecordsResult> getRecordsResultFuture = futures.get(partition);
                    if (getRecordsResultFuture != null) {
                        while (!getRecordsResultFuture.isDone() && !forceWakeup) {
                            Utils.sleep(1);
                        }

                        if (getRecordsResultFuture.isDone()) {
                            futures.remove(partition);
                            receivedCnt.decrementAndGet();
                        }
                    }
                }
            }
        }

        if(needUpdateOffset.size() > 0) {
            coordinator.seek(needUpdateOffset);
        }

        for (StreamPartition partition : subscriptions.fetchablePartitions()) {
            GetRecordsRequest getRecordsParam = new GetRecordsRequest();
            if (nextIterators.get(partition) == null || nextIterators.get(partition).isExpire()) {
                log.warn("partition " + partition + " next cursor is null, can not send fetch request");
                continue;
            }
            getRecordsParam.setPartitionCursor(nextIterators.get(partition).getNextPartitionCursor());

            if (futures.get(partition) == null) {
                futures.put(partition, disAsync.getRecordsAsync(getRecordsParam, new AsyncHandler<GetRecordsResult>() {
                    @Override
                    public void onError(Exception exception) throws Exception {
                        Integer errorCount = partitionException.get(partition);
                        if (errorCount == null) {
                            errorCount = 0;
                        }

                        Double sleepTime = 100 * Math.pow(2, errorCount);
                        sleepTime = sleepTime > disConfig.getBackOffMaxIntervalMs() ? disConfig.getBackOffMaxIntervalMs() : sleepTime;
                        Utils.sleep(sleepTime.intValue());

                        receivedCnt.getAndIncrement();
                        log.warn("Failed to getRecordsAsync, error : [{}], request : [{}]",
                                exception.getMessage(), new String(Base64.getUrlDecoder().decode(getRecordsParam.getPartitionCursor())));

                        // 将异常抛出，由外层处理
                        throw exception;
                    }

                    @Override
                    public void onSuccess(GetRecordsResult getRecordsResult) {
                        receivedCnt.getAndIncrement();
                        log.debug("{} get {} records.", partition, getRecordsResult.getRecords().size());
                    }
                }));
            }
        }
    }

    public FetchResult fetchRecords(long timeout) {
        Map<StreamPartition, List<Record>> multipleRecords = new HashMap<>();
        long totalRecords = 0L;
        long now = System.currentTimeMillis();
        forceWakeup = false;
        while (System.currentTimeMillis() - now <= timeout && receivedCnt.get() == 0 && !forceWakeup) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        if (receivedCnt.get() > 0) {
            for (StreamPartition partition : futures.keySet()) {
                Future<GetRecordsResult> getRecordsResultFuture = futures.get(partition);
                if (getRecordsResultFuture != null && getRecordsResultFuture.isDone()) {
                    if (subscriptions.isAssigned(partition) && !subscriptions.isPaused(partition)) {
                        try {
                            GetRecordsResult getRecordsResult = getRecordsResultFuture.get();
                            List<Record> records = getRecordsResult.getRecords();
                            multipleRecords.putIfAbsent(partition, new ArrayList<>());
                            multipleRecords.get(partition).addAll(records);
                            nextIterators.put(partition, new PartitionCursor(getRecordsResult.getNextPartitionCursor()));
                            if (!records.isEmpty()) {
                                totalRecords += records.size();
                                subscriptions.seek(partition,
                                        Long.valueOf(records.get(records.size() - 1).getSequenceNumber()) + 1L);
                            }
                            partitionException.remove(partition);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage(), e);
                        } catch (ExecutionException e) {
                            Throwable cause = e.getCause() == null ? e : e.getCause();
                            if (cause instanceof DISPartitionNotExistsException || cause instanceof DISPartitionExpiredException) {
                                // 对于分区不存在/过期异常，可以重试几次看是否恢复
                                Integer errorCount = partitionException.get(partition);
                                log.warn("Find retriable error {}, errorCount {}", cause.getMessage(), (errorCount == null ? 1 : errorCount));
                                if (errorCount == null) {
                                    partitionException.put(partition, 2);
                                } else if (errorCount < 5) {
                                    // tolerate N times failure
                                    partitionException.put(partition, (errorCount + 1));
                                } else {
                                    partitionException.remove(partition);
                                    if (subscriptions.partitionsAutoAssigned()) {
                                        // 使用subscribe机制时，重新分配组即可
                                        coordinator.requestRebalance();
                                    } else if (totalRecords == 0) {
                                        // 如果是assign模式且没有获取到记录，直接抛出异常
                                        throw (DISClientException) cause;
                                    } else {
                                        // 先记录异常，等待下一次poll再抛出
                                        exception = (DISClientException) cause;
                                    }
                                }
                            } else {
                                log.error(cause.getMessage(), cause);
                                partitionException.remove(partition);
                                DISClientException disClientException;
                                if (cause instanceof DISClientException) {
                                    disClientException = (DISClientException) cause;
                                } else {
                                    disClientException = new DISClientException(cause);
                                }
                                if (totalRecords == 0) {
                                    throw disClientException;
                                } else {
                                    exception = disClientException;
                                }
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                            partitionException.remove(partition);
                            DISClientException disClientException;
                            if (e instanceof DISClientException) {
                                disClientException = (DISClientException) e;
                            } else {
                                disClientException = new DISClientException(e);
                            }
                            if (totalRecords == 0) {
                                throw disClientException;
                            } else {
                                exception = disClientException;
                            }
                        } finally {
                            futures.remove(partition);
                            receivedCnt.decrementAndGet();
                        }
                    } else {
                        nextIterators.remove(partition);
                        futures.remove(partition);
                        receivedCnt.decrementAndGet();
                    }
                }
            }
        }
        return new FetchResult(multipleRecords, totalRecords);
    }

    public void wakeup() {
        this.forceWakeup = true;
    }

    public void throwIfFetchError() {
        if (exception != null) {
            DISClientException e = exception;
            exception = null;
            throw e;
        }
    }

    class FetchResult {
        private Map<StreamPartition, List<Record>> records;
        private long recordsCount;

        public FetchResult(Map<StreamPartition, List<Record>> records, long recordsCount) {
            this.records = records;
            this.recordsCount = recordsCount;
        }

        public Map<StreamPartition, List<Record>> getRecords() {
            return records;
        }

        public long getRecordsCount() {
            return recordsCount;
        }
    }
}
