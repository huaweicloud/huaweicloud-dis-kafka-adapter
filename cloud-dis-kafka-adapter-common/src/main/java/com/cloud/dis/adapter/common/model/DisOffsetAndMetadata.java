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

package com.cloud.dis.adapter.common.model;

import java.io.Serializable;

public class DisOffsetAndMetadata implements Serializable {
    private final long offset;
    private final String metadata;

    public DisOffsetAndMetadata(long offset, String metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    public DisOffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DisOffsetAndMetadata that = (DisOffsetAndMetadata) o;

        if (offset != that.offset) return false;
        return metadata == null ? that.metadata == null : metadata.equals(that.metadata);

    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{offset=" + offset + ", metadata='" + metadata + "'}";
    }
}
