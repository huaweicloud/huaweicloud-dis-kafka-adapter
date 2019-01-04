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
package com.huaweicloud.dis.adapter.kafka.common.serialization;

import com.huaweicloud.dis.adapter.kafka.common.utils.Bytes;

import java.util.Map;

public class BytesDeserializer implements Deserializer<Bytes> {

    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    public Bytes deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        return new Bytes(data);
    }

    public void close() {
        // nothing to do
    }
}
