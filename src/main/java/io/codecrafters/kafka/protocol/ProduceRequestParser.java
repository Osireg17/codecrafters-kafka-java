package io.codecrafters.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ProduceRequestParser {

    public static class ProduceData {
        private final String topicName;
        private final int partitionIndex;

        public ProduceData(String topicName, int partitionIndex) {
            this.topicName = topicName;
            this.partitionIndex = partitionIndex;
        }

        public String getTopicName() {
            return topicName;
        }

        public int getPartitionIndex() {
            return partitionIndex;
        }
    }

    /**
     * Parse Produce request v11 to extract the first topic name and partition index.
     *
     * @param buffer The request buffer positioned after the request header
     * @return ProduceData containing the first topic name and partition index
     */
    public static ProduceData parse(ByteBuffer buffer) {
        // Skip client_id (nullable string) in request header v2
        buffer.position(12);
        short clientIdLength = buffer.getShort();
        if (clientIdLength > 0) {
            buffer.position(buffer.position() + clientIdLength);
        }

        // Skip request header tagged fields
        int headerTaggedFieldsLength = readUnsignedVarInt(buffer);
        if (headerTaggedFieldsLength > 0) {
            buffer.position(buffer.position() + headerTaggedFieldsLength);
        }

        // Skip transactional_id (compact nullable string)
        int transactionalIdLength = readUnsignedVarInt(buffer) - 1;
        if (transactionalIdLength > 0) {
            buffer.position(buffer.position() + transactionalIdLength);
        }

        // Skip acks (int16)
        buffer.position(buffer.position() + 2);

        // Skip timeout_ms (int32)
        buffer.position(buffer.position() + 4);

        // Read topics array length (compact array)
        int topicsArrayLength = readUnsignedVarInt(buffer) - 1;
        if (topicsArrayLength <= 0) {
            return null;
        }

        // Read first topic name (compact string)
        int topicNameLength = readUnsignedVarInt(buffer) - 1;
        byte[] topicNameBytes = new byte[topicNameLength];
        buffer.get(topicNameBytes);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);

        // Read partition array length (compact array)
        int partitionsArrayLength = readUnsignedVarInt(buffer) - 1;
        if (partitionsArrayLength <= 0) {
            return null;
        }

        // Read first partition index (int32)
        int partitionIndex = buffer.getInt();

        // We don't need to parse further for the error response
        return new ProduceData(topicName, partitionIndex);
    }

    private static int readUnsignedVarInt(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        while (true) {
            int b = Byte.toUnsignedInt(buffer.get());
            value |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalArgumentException("Varint too long");
            }
        }
    }
}

