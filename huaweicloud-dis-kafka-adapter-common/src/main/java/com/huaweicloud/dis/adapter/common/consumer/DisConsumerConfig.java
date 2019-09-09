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

public class DisConsumerConfig extends DISConfig {
    public static final String GROUP_ID_CONFIG = DISConfig.GROUP_ID;

    /**
     * 是否启用定时心跳，默认开启</br>
     * 定时心跳周期为 {@value #HEARTBEAT_INTERVAL_MS_CONFIG}
     */
    public static final String ENABLE_PERIODIC_HEARTBEAT_CONFIG = "heartbeat.periodic.heartbeat";

    /**
     * 定时心跳发送周期</br>
     * 定时心跳开关通过 {@value ENABLE_PERIODIC_HEARTBEAT_CONFIG} 启用
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";

    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";

    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";

    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";

    public static final String CLIENT_ID_CONFIG = "client.id";

    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";

    public static final String MAX_FETCH_THREADS_CONFIG = "max.fetch.threads";

    public static final String ENABLE_EXCEPTION_AUTO_RETRY_CONFIG = "poll.exception.auto.retry";

    public static final String PROPERTY_EXCEPTION_RETRY_NUM = "poll.exception.retry.num";

    public static final String PROPERTY_EXCEPTION_RETRY_WAIT_TIME_MS = "poll.exception.retry.wait.time.ms";

    /**
     * 是否开启分配加速，默认不开启，建议只有单个Consumer时开启
     */
    public static final String ENABLE_ACCELERATE_ASSIGN_CONFIG = "enable.accelerate.assign";
}
