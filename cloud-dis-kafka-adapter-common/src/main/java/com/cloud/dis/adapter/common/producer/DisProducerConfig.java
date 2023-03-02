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
package com.cloud.dis.adapter.common.producer;

import com.cloud.dis.DISConfig;

public class DisProducerConfig extends DISConfig {

    public static final String CLIENT_ID_CONFIG = "client.id";

    public static final String BATCH_SIZE_CONFIG = DISConfig.PROPERTY_PRODUCER_BATCH_SIZE;

    public static final String BUFFER_MEMORY_CONFIG = DISConfig.PROPERTY_PRODUCER_BUFFER_MEMORY;

    public static final String BATCH_COUNT_CONFIG = DISConfig.PROPERTY_PRODUCER_BATCH_COUNT;

    public static final String BUFFER_COUNT_CONFIG = DISConfig.PROPERTY_PRODUCER_BUFFER_COUNT;

    public static final String BLOCK_ON_BUFFER_FULL_CONFIG = DISConfig.PROPERTY_PRODUCER_BLOCK_ON_BUFFER_FULL;

    public static final String LINGER_MS_CONFIG = DISConfig.PROPERTY_PRODUCER_LINGER_MS;

    public static final String MAX_BLOCK_MS_CONFIG = DISConfig.PROPERTY_PRODUCER_MAX_BLOCK_MS;

    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = DISConfig.PROPERTY_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;

    public static final String RETRIES_CONFIG = DISConfig.PROPERTY_PRODUCER_EXCEPTION_RETRIES;

    public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";

    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
}
