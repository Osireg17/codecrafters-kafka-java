package io.codecrafters.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ProduceRequestParser {

    /**
     * Represents a single partition in a Produce request with its record batch.
     */
    public static class ProducePartition {
        private final int partitionIndex;
        private final byte[] recordBatchBytes;

        public ProducePartition(int partitionIndex, byte[] recordBatchBytes) {
            this.partitionIndex = partitionIndex;
            this.recordBatchBytes = recordBatchBytes;
        }

        public int getPartitionIndex() {
            return partitionIndex;
        }

        public byte[] getRecordBatchBytes() {
            return recordBatchBytes;
        }
    }

    /**
     * Represents a topic in a Produce request with its partitions.
     */
    public static class ProduceTopic {
        private final String topicName;
        private final List<ProducePartition> partitions;

        // Legacy fields for backward compatibility
        private final int partitionIndex;
        private final byte[] recordBatchBytes;

        // Legacy constructor (backward compatibility)
        public ProduceTopic(String topicName, int partitionIndex) {
            this.topicName = topicName;
            this.partitionIndex = partitionIndex;
            this.recordBatchBytes = null;
            this.partitions = new ArrayList<>();
        }

        // Legacy constructor with records (backward compatibility)
        public ProduceTopic(String topicName, int partitionIndex, byte[] recordBatchBytes) {
            this.topicName = topicName;
            this.partitionIndex = partitionIndex;
            this.recordBatchBytes = recordBatchBytes;
            this.partitions = new ArrayList<>();
        }

        // New constructor for multi-partition support
        public ProduceTopic(String topicName, List<ProducePartition> partitions) {
            this.topicName = topicName;
            this.partitions = partitions;
            // Set legacy fields to first partition if available
            if (!partitions.isEmpty()) {
                this.partitionIndex = partitions.get(0).getPartitionIndex();
                this.recordBatchBytes = partitions.get(0).getRecordBatchBytes();
            } else {
                this.partitionIndex = -1;
                this.recordBatchBytes = null;
            }
        }

        public String getTopicName() {
            return topicName;
        }

        public List<ProducePartition> getPartitions() {
            return partitions;
        }

        // Legacy getter (backward compatibility)
        public int getPartitionIndex() {
            return partitionIndex;
        }

        // Legacy getter (backward compatibility)
        public byte[] getRecordBatchBytes() {
            return recordBatchBytes;
        }
    }

    /**
     * Parse Produce request v11 to extract ALL topics, partitions, and record batch bytes.
     *
     * @param buffer The request buffer positioned after the request header
     * @return List of ProduceTopic objects, each containing topic name and list of partitions with records
     */
    public static List<ProduceTopic> parseAllTopics(ByteBuffer buffer) {
        List<ProduceTopic> topics = new ArrayList<>();

        // Step 1: Position buffer at start of request header fields
        buffer.position(12);

        // Step 2: Read client_id as compact string; advance by its length
        short clientIdLength = buffer.getShort();
        if (clientIdLength > 0) {
            buffer.position(buffer.position() + clientIdLength);
        }

        // Step 3: Read header tagged_fields length; advance
        int headerTaggedFieldsLength = readUnsignedVarInt(buffer);
        if (headerTaggedFieldsLength > 0) {
            buffer.position(buffer.position() + headerTaggedFieldsLength);
        }

        // Step 4: Read transactional_id as compact nullable string; advance
        int transactionalIdLength = readUnsignedVarInt(buffer) - 1;
        if (transactionalIdLength > 0) {
            buffer.position(buffer.position() + transactionalIdLength);
        }

        // Step 5: Read acks and timeout_ms; advance
        buffer.position(buffer.position() + 2); // acks (int16)
        buffer.position(buffer.position() + 4); // timeout_ms (int32)

        // Step 6: Read topics array length (compact array)
        int topicsArrayLength = readUnsignedVarInt(buffer) - 1;

        // Handle empty topics array
        if (topicsArrayLength <= 0) {
            return topics;
        }

        // Step 7: Repeat for each topic
        for (int topicIdx = 0; topicIdx < topicsArrayLength; topicIdx++) {
            // Step 7a: Read topic name (compact string)
            int topicNameLength = readUnsignedVarInt(buffer) - 1;
            if (topicNameLength <= 0) {
                continue; // Skip invalid topic
            }

            byte[] topicNameBytes = new byte[topicNameLength];
            buffer.get(topicNameBytes);
            String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);

            // Step 7b: Read partitions array length (compact array)
            int partitionsArrayLength = readUnsignedVarInt(buffer) - 1;

            List<ProducePartition> partitions = new ArrayList<>();

            // Step 7c: Repeat for each partition
            if (partitionsArrayLength > 0) {
                for (int partIdx = 0; partIdx < partitionsArrayLength; partIdx++) {
                    // Read partition index (int32)
                    int partitionIndex = buffer.getInt();

                    // Read records length (compact bytes)
                    int recordsLength = readUnsignedVarInt(buffer) - 1;

                    // Slice recordBatchBytes
                    byte[] recordBatchBytes;
                    if (recordsLength > 0) {
                        recordBatchBytes = new byte[recordsLength];
                        buffer.get(recordBatchBytes);
                    } else {
                        recordBatchBytes = new byte[0];
                    }

                    // Store partitionIndex + recordBatchBytes
                    partitions.add(new ProducePartition(partitionIndex, recordBatchBytes));

                    // Read partition tagged_fields length; advance
                    int partitionTaggedFieldsLength = readUnsignedVarInt(buffer);
                    if (partitionTaggedFieldsLength > 0) {
                        buffer.position(buffer.position() + partitionTaggedFieldsLength);
                    }
                }
            }

            // Step 7d: Read topic tagged_fields length; advance
            int topicTaggedFieldsLength = readUnsignedVarInt(buffer);
            if (topicTaggedFieldsLength > 0) {
                buffer.position(buffer.position() + topicTaggedFieldsLength);
            }

            // Step 7e: Store topic + partitions
            topics.add(new ProduceTopic(topicName, partitions));
        }

        // Step 8: Return parsed topics list
        return topics;
    }

    /**
     * Parse Produce request v11 to extract the first topic name and partition index.
     *
     * @param buffer The request buffer positioned after the request header
     * @return ProduceData containing the first topic name and partition index
     */
    public static ProduceTopic parseTopic(ByteBuffer buffer) {
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
        return new ProduceTopic(topicName, partitionIndex);
    }

    /**
     * Parse Produce request v11 to extract topic name, partition index, and record batch bytes.
     *
     * @param buffer The request buffer positioned after the request header
     * @return ProduceTopic containing topic name, partition index, and record batch bytes
     */
    public static ProduceTopic parseTopicWithRecords(ByteBuffer buffer) {
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
        if (topicNameLength <= 0) {
            return null;
        }
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

        // Read records length (compact bytes = unsigned varint)
        int recordsLength = readUnsignedVarInt(buffer) - 1;

        // Handle empty or invalid records
        if (recordsLength <= 0) {
            return new ProduceTopic(topicName, partitionIndex, new byte[0]);
        }

        // Read record batch bytes
        byte[] recordBatchBytes = new byte[recordsLength];
        buffer.get(recordBatchBytes);

        return new ProduceTopic(topicName, partitionIndex, recordBatchBytes);
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

