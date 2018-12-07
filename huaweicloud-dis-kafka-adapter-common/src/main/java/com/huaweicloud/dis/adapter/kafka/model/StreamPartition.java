package com.huaweicloud.dis.adapter.kafka.model;

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
