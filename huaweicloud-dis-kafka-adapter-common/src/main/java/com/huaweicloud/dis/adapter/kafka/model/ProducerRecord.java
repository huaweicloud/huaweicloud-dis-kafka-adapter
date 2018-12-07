package com.huaweicloud.dis.adapter.kafka.model;

public final class ProducerRecord {

    private final String stream;
    private final Integer partition;
    private final String key;
    private final byte[] value;
    private final Long timestamp;


    public ProducerRecord(String stream, Integer partition, Long timestamp, String key, byte[] value) {
        if (stream == null)
            throw new IllegalArgumentException("Topic cannot be null");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp " + timestamp);
        this.stream = stream;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }


    public ProducerRecord(String stream, Integer partition, String key, byte[] value) {
        this(stream, partition, null, key, value);
    }


    public ProducerRecord(String stream, String key, byte[] value) {
        this(stream, null, null, key, value);
    }


    public ProducerRecord(String stream, byte[] value) {
        this(stream, null, null, null, value);
    }


    public String stream() {
        return stream;
    }


    public String key() {
        return key;
    }


    public byte[] value() {
        return value;
    }


    public Long timestamp() {
        return timestamp;
    }


    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(stream=" + stream + ", partition=" + partition + ", key=" + key + ", value=" + value +
                ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerRecord))
            return false;

        ProducerRecord that = (ProducerRecord) o;

        if (key != null ? !key.equals(that.key) : that.key != null)
            return false;
        else if (partition != null ? !partition.equals(that.partition) : that.partition != null)
            return false;
        else if (stream != null ? !stream.equals(that.stream) : that.stream != null)
            return false;
        else if (value != null ? !value.equals(that.value) : that.value != null)
            return false;
        else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = stream != null ? stream.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
