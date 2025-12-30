package io.codecrafters.kafka.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Writer for appending record batches to Kafka partition log files.
 * Handles directory creation and file append operations.
 */
public class PartitionLogWriter {

    private final String logBaseDirectory;
    private static final String LOG_FILE_NAME = "00000000000000000000.log";

    /**
     * Create a PartitionLogWriter with the specified base log directory.
     *
     * @param logBaseDirectory The base directory for all topic partition logs
     */
    public PartitionLogWriter(String logBaseDirectory) {
        this.logBaseDirectory = logBaseDirectory;
    }

    /**
     * Append a record batch to the partition log file.
     * Creates the partition directory and log file if they don't exist.
     *
     * @param topicName The name of the topic
     * @param partitionIndex The partition index
     * @param recordBatchBytes The record batch bytes to append
     * @throws IOException If an I/O error occurs
     */
    public void appendRecordBatch(String topicName, int partitionIndex, byte[] recordBatchBytes) throws IOException {
        // Step 1: Build partition directory path
        String partitionDirName = topicName + "-" + partitionIndex;
        Path partitionDirPath = Paths.get(logBaseDirectory, partitionDirName);

        // Step 2: Create directory if it doesn't exist
        if (!Files.exists(partitionDirPath)) {
            System.out.println("Creating partition directory: " + partitionDirPath);
            Files.createDirectories(partitionDirPath);
        }

        // Step 3: Build log file path
        Path logFilePath = partitionDirPath.resolve(LOG_FILE_NAME);

        // Step 4 & 5: Open file in append mode and write record batch bytes
        // StandardOpenOption.CREATE creates the file if it doesn't exist
        // StandardOpenOption.APPEND appends to the file
        System.out.println("Appending " + recordBatchBytes.length + " bytes to: " + logFilePath);
        Files.write(logFilePath, recordBatchBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        // Step 6: File is automatically closed by Files.write
        System.out.println("Successfully appended record batch to partition log");
    }

    /**
     * Append a record batch to the partition log file (alternative implementation using FileOutputStream).
     * This method provides explicit control over file operations.
     *
     * @param topicName The name of the topic
     * @param partitionIndex The partition index
     * @param recordBatchBytes The record batch bytes to append
     * @throws IOException If an I/O error occurs
     */
    public void appendRecordBatchWithStream(String topicName, int partitionIndex, byte[] recordBatchBytes) throws IOException {
        // Step 1: Build partition directory path
        String partitionDirName = topicName + "-" + partitionIndex;
        File partitionDir = new File(logBaseDirectory, partitionDirName);

        // Step 2: Create directory if it doesn't exist
        if (!partitionDir.exists()) {
            System.out.println("Creating partition directory: " + partitionDir.getAbsolutePath());
            if (!partitionDir.mkdirs()) {
                throw new IOException("Failed to create partition directory: " + partitionDir.getAbsolutePath());
            }
        }

        // Step 3: Build log file path
        File logFile = new File(partitionDir, LOG_FILE_NAME);

        // Step 4: Open file in append mode
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            // true = append mode

            // Step 5: Write record batch bytes
            System.out.println("Appending " + recordBatchBytes.length + " bytes to: " + logFile.getAbsolutePath());
            fos.write(recordBatchBytes);
            fos.flush();

            System.out.println("Successfully appended record batch to partition log");
        }
        // Step 6: Close file
    }
}
