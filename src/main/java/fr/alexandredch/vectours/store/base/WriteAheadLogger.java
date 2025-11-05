package fr.alexandredch.vectours.store.base;

import java.util.List;

public final class WriteAheadLogger {

    public static final String LOG_FILE_NAME = "vectours_wal.log";

    public List<Operation> loadFromCheckpoint() {
        // Read from the last checkpoint and return the list of operations
        return List.of();
    }

    public void applyOperation(Operation operation) {
        switch (operation) {
            case Insert insert -> {
                // Write insert operation to log
            }
            case Delete delete -> {
                // Write delete operation to log
            }
        }
    }

    public void markLastCheckpoint() {
        // Edit the log to mark the last checkpoint
    }

    public sealed interface Operation permits Insert, Delete {
    }

    record Insert(String id, byte[] vectorBytes) implements Operation {
    }

    record Delete(String id) implements Operation {
    }
}
