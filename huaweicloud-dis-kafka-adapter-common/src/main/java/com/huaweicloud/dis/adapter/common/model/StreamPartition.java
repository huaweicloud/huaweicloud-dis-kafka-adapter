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

package com.huaweicloud.dis.adapter.common.model;

import java.io.Serializable;

/**
 * A stream name and partition number
 */
public final class StreamPartition implements Serializable {

    private int hash = 0;
    private final int partition;
    private final String stream;

    public StreamPartition(String stream, int partition) {
        this.partition = partition;
        this.stream = stream;
    }

    public int partition() {
        return partition;
    }

    public String stream() {
        return stream;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + partition;
        result = prime * result + ((stream == null) ? 0 : stream.hashCode());
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StreamPartition other = (StreamPartition) obj;
        if (partition != other.partition)
            return false;
        if (stream == null) {
            if (other.stream != null)
                return false;
        } else if (!stream.equals(other.stream))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return stream + "-" + partition;
    }

}
