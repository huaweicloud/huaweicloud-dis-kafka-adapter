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
package com.cloud.dis.adapter.kafka.common.serialization;

import java.nio.ByteBuffer;
import java.util.Map;

public class ByteBufferSerializer implements Serializer<ByteBuffer> {

    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    public byte[] serialize(String topic, ByteBuffer data) {
        if (data == null)
            return null;

        data.rewind();

        if (data.hasArray()) {
            byte[] arr = data.array();
            if (data.arrayOffset() == 0 && arr.length == data.remaining()) {
                return arr;
            }
        }

        byte[] ret = new byte[data.remaining()];
        data.get(ret, 0, ret.length);
        data.rewind();
        return ret;
    }

    public void close() {
        // nothing to do
    }
}
