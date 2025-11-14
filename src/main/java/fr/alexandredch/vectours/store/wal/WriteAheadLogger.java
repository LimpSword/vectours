package fr.alexandredch.vectours.store.wal;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.store.base.Segment;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class WriteAheadLogger {

    public static final String LOG_FILE_NAME = "vectours_wal.log";
    public static final String CHECKPOINT_FILE_NAME = "vectours_wal_checkpoint.dat";

    public static final byte[] SEPARATOR_BYTES = "\n".getBytes();

    private final WALWriterBatcher walWriterBatcher = new WALWriterBatcher();

    public List<Operation> loadFromCheckpoint() {
        // Read from the last checkpoint and return the list of operations
        int lastCheckpointedSegmentId = getLastCheckpointedSegmentId();
        List<Operation> operations = new ArrayList<>();
        boolean startCollecting = lastCheckpointedSegmentId == -1;
        boolean next = false;
        int currentSegmentId = -1;
        for (byte[] line : rawLines()) {
            try {
                int segmentId = Integer.parseInt(new String(line));
                currentSegmentId = segmentId;
                if (segmentId == lastCheckpointedSegmentId) {
                    next = true;
                } else if (next) {
                    startCollecting = true;
                }

                if (startCollecting) {
                    operations.add(new Operation.CreateSegment(segmentId));
                }
            } catch (NumberFormatException e) {
                // Ignore non-segment id lines
            }
            if (startCollecting) {
                try {
                    Operation operation = Operation.fromBytes(line);
                    if (operation instanceof Operation.Insert(Vector vector)) {
                        operation = new Operation.InsertInSegment(vector, currentSegmentId);
                    }
                    operations.add(operation);
                } catch (Exception e) {
                    // Ignore invalid operation lines
                }
            }
        }
        return operations;
    }

    public void newSegment(Segment segment) {
        // Add the segment id to the log
        Path path = Paths.get(LOG_FILE_NAME);
        try {
            Files.write(
                    path,
                    Integer.toString(segment.getId()).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            Files.write(path, SEPARATOR_BYTES, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL log", e);
        }
    }

    public CompletableFuture<Void> applyOperation(Operation operation) {
        WALWriterBatcher.BatchItem batchItem = new WALWriterBatcher.BatchItem(operation, new CompletableFuture<>());
        walWriterBatcher.offer(batchItem);

        return batchItem.future().thenAccept(result -> {
            if (result.exception() != null) {
                throw new RuntimeException("Failed to write to WAL log", result.exception());
            }
        });
    }

    public int getLastCheckpointedSegmentId() {
        Path path = Paths.get(CHECKPOINT_FILE_NAME);
        if (!Files.exists(path)) {
            return -1;
        }
        try {
            String content = Files.readString(path);
            return Integer.parseInt(content);
        } catch (IOException | NumberFormatException e) {
            throw new RuntimeException("Failed to read WAL checkpoint", e);
        }
    }

    public void markLastCheckpoint(Segment segment) {
        // Update the checkpoint file with the latest segment id
        Path path = Paths.get(CHECKPOINT_FILE_NAME);
        try {
            Files.write(
                    path,
                    Integer.toString(segment.getId()).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write WAL checkpoint", e);
        }

        // TODO: truncate the log file to the last checkpointed segment id
    }

    public int getLatestSegmentIdIncludingUnclosed() {
        int latestSegmentId = -1;
        for (byte[] line : rawLines()) {
            try {
                int segmentId = Integer.parseInt(new String(line));
                if (segmentId > latestSegmentId) {
                    latestSegmentId = segmentId;
                }
            } catch (NumberFormatException e) {
                // Ignore non-segment id lines
            }
        }
        return latestSegmentId;
    }

    public void clearLog() {
        Path logFilePath = Paths.get(LOG_FILE_NAME);
        Path checkpointFilePath = Paths.get(CHECKPOINT_FILE_NAME);
        try {
            Files.deleteIfExists(logFilePath);
            Files.deleteIfExists(checkpointFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear WAL log", e);
        }
    }

    private List<byte[]> rawLines() {
        try {
            Path path = Paths.get(LOG_FILE_NAME);
            if (!Files.exists(path)) {
                return List.of();
            }

            // Get all bytes separated by new lines
            List<byte[]> rawLines = new ArrayList<>();
            List<Byte> currentLine = new ArrayList<>();
            byte[] allBytes = Files.readAllBytes(path);
            for (byte b : allBytes) {
                if (b == '\n') {
                    byte[] lineBytes = new byte[currentLine.size()];
                    for (int i = 0; i < currentLine.size(); i++) {
                        lineBytes[i] = currentLine.get(i);
                    }
                    rawLines.add(lineBytes);
                    currentLine.clear();
                } else {
                    currentLine.add(b);
                }
            }
            return rawLines;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read WAL log", e);
        }
    }

    public void shutdown() {
        walWriterBatcher.shutdown();
    }
}
