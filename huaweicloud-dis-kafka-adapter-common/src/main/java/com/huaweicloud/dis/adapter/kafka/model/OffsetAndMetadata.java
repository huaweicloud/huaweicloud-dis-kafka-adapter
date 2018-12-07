package com.huaweicloud.dis.adapter.kafka.model;

import java.io.Serializable;

public class OffsetAndMetadata implements Serializable {
    private final long offset;
    private final String metadata;

    public OffsetAndMetadata(long offset, String metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    public OffsetAndMetadata(long offset) {
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

        OffsetAndMetadata that = (OffsetAndMetadata) o;

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
        return "OffsetAndMetadata{" +
                "offset=" + offset +
                ", metadata='" + metadata + '\'' +
                '}';
    }
}
