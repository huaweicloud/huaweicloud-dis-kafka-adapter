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

    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";

    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";

    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";

    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";

    public static final String CLIENT_ID_CONFIG = "client.id";

    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";

    public static final String MAX_FETCH_THREADS_CONFIG = "max.fetch.threads";

    public static final String MAX_PARTITION_FETCH_RECORDS_CONFIG = "max.partition.fetch.records";
}
