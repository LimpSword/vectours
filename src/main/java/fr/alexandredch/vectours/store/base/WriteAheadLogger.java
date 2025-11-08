package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.operations.Operation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class WriteAheadLogger {

    public static final String LOG_FILE_NAME = "vectours_wal.log";
    public static final String CHECKPOINT_FILE_NAME = "vectours_wal_checkpoint.dat";

    public List<Operation> loadFromCheckpoint() {
        // Read from the last checkpoint and return the list of operations
        int lastCheckpointedSegmentId = getLastCheckpointedSegmentId();
        Path path = Paths.get(LOG_FILE_NAME);
        if (!Files.exists(path)) {
            return List.of();
        }
        try {
            List<String> lines = Files.readAllLines(path);
            List<Operation> operations = new ArrayList<>();
            boolean startCollecting = lastCheckpointedSegmentId == -1;
            boolean next = false;
            for (String line : lines) {
                if (!startCollecting) {
                    try {
                        int segmentId = Integer.parseInt(line);
                        if (next) {
                            startCollecting = true;
                        }
                        if (segmentId == lastCheckpointedSegmentId) {
                            next = true;
                        }
                    } catch (NumberFormatException e) {
                        // Ignore non-segment id lines
                    }
                } else {
                    try {
                        Operation operation = Operation.fromBytes(line.getBytes());
                        operations.add(operation);
                    } catch (Exception e) {
                        // Ignore invalid operation lines
                    }
                }
            }
            return operations;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read WAL log", e);
        }
    }

    public void newSegment(Segment segment) {
        // Add the segment id to the log
        Path path = Paths.get(LOG_FILE_NAME);
        try {
            Files.write(
                    path,
                    List.of(Integer.toString(segment.getId())),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL log", e);
        }
    }

    public void applyOperation(Operation operation) {
        // Add the segment id to the log
        Path path = Paths.get(LOG_FILE_NAME);
        try {
            Files.write(path, operation.toBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Files.write(path, List.of("\n"), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL log", e);
        }
    }

    public int getLastCheckpointedSegmentId() {
        Path path = Paths.get(CHECKPOINT_FILE_NAME);
        if (!Files.exists(path)) {
            return -1;
        }
        try {
            String content = Files.readString(path).trim();
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
                    List.of(Integer.toString(segment.getId())),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write WAL checkpoint", e);
        }
    }

    public int getLatestSegmentIdIncludingUnclosed() {
        Path path = Paths.get(LOG_FILE_NAME);
        if (!Files.exists(path)) {
            return -1;
        }
        try {
            List<String> lines = Files.readAllLines(path);
            int maxId = -1;
            for (String line : lines) {
                try {
                    int segmentId = Integer.parseInt(line);
                    if (segmentId > maxId) {
                        maxId = segmentId;
                    }
                } catch (NumberFormatException e) {
                    // Ignore non-segment id lines
                }
            }
            return maxId;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read WAL log", e);
        }
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
}
