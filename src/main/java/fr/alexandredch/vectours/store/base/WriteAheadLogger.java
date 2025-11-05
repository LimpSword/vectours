package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.operations.Operation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public final class WriteAheadLogger {

    public static final String LOG_FILE_NAME = "vectours_wal.log";
    public static final String CHECKPOINT_FILE_NAME = "vectours_wal_checkpoint.dat";

    public List<Operation> loadFromCheckpoint() {
        // Read from the last checkpoint and return the list of operations
        return List.of();
    }

    public void newSegment(Segment segment) {
        // Add the segment id to the log
        appendToLog(Integer.toString(segment.getId()).getBytes());
    }

    public void applyOperation(Operation operation) {
        appendToLog(operation.toBytes());
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
    }

    public int getLatestSegmentIdIncludingUnclosed() {
        Path path = Paths.get(LOG_FILE_NAME);
        if (!Files.exists(path)) {
            return -1;
        }
        try {
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read WAL log", e);
        }
    }

    public void clearLog() {
        Path path = Paths.get(LOG_FILE_NAME);
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear WAL log", e);
        }
    }

    private void appendToLog(byte[] data) {
        Path path = Paths.get(LOG_FILE_NAME);
        try {
            Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL log", e);
        }
    }
}
