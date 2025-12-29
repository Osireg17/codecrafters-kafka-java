package io.codecrafters.kafka.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaResponse {
    private final List<Byte> data;
    private final int correlationId;

    public KafkaResponse(int correlationId) {
        this.data = new ArrayList<>();
        this.correlationId = correlationId;
        initializeHeader();
    }

    private void initializeHeader() {
        ByteUtils.fillBytes(data, (byte) 0, 4);
        ByteUtils.fillBytes(data, correlationId, 4);
    }

    public void addBytes(byte value, int count) {
        ByteUtils.fillBytes(data, value, count);
    }

    public void addBytes(long value, int length) {
        ByteUtils.fillBytes(data, value, length);
    }

    public void addBytes(byte[] bytes) {
        ByteUtils.fillBytes(data, bytes);
    }

    public void addBytes(List<Byte> bytes) {
        data.addAll(bytes);
    }

    public void addByte(byte value) {
        data.add(value);
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(data.size());
        buffer.position(0);
        data.forEach(buffer::put);
        buffer.putInt(0, data.size() - 4);
        buffer.position(0);
        return buffer;
    }
}
