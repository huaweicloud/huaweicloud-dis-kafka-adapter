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

import java.io.Closeable;
import java.util.Map;

/**
 * An interface for converting bytes to objects.
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 * <p>
 * Implement {@link com.huaweicloud.dis.adapter.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 *
 * @param <T> Type to be deserialized into.
 */
public interface Deserializer<T> extends Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Deserialize a record value from a byte array into a value or object.
     * @param topic topic associated with the data
     * @param data serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    T deserialize(String topic, byte[] data);

    @Override
    void close();
}
