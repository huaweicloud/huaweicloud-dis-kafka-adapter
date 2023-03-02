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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public abstract class DelayedTask implements Delayed {

    long startTimeMs;

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public DelayedTask(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = startTimeMs - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(startTimeMs, ((DelayedTask) o).startTimeMs);
    }

    abstract void doRequest();
}
