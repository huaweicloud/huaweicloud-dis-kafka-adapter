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

package com.cloud.dis.adapter.common.consumer;

public final class PartitionCursor {
    public static final long PARTITION_CURSOR_EXPIRE_MS = 270000;

    private String nextPartitionCursor;

    private long updateTimestamp;

    public PartitionCursor(String nextPartitionCursor, long updateTimestamp) {
        this.nextPartitionCursor = nextPartitionCursor;
        this.updateTimestamp = updateTimestamp;
    }

    public PartitionCursor(String nextPartitionCursor) {
        this.nextPartitionCursor = nextPartitionCursor;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public String getNextPartitionCursor() {
        return nextPartitionCursor;
    }

    public void setNextPartitionCursor(String nextPartitionCursor) {
        this.nextPartitionCursor = nextPartitionCursor;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public boolean isExpire() {
        return (System.currentTimeMillis() - updateTimestamp) > PARTITION_CURSOR_EXPIRE_MS;
    }
}