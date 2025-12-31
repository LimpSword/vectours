package fr.alexandredch.vectours.store.wal;

import com.google.common.primitives.Bytes;
import fr.alexandredch.vectours.operations.Operation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WALWriterBatcher {

    private static final Logger logger = LoggerFactory.getLogger(WALWriterBatcher.class);

    private static final int MAX_BATCH_SIZE = 500;
    private static final Path path = Paths.get(WriteAheadLogger.LOG_FILE_NAME);

    private final BlockingQueue<BatchItem> queue = new LinkedBlockingQueue<>();
    private final Thread processingThread;
    private volatile boolean running = true;

    public WALWriterBatcher() {
        this.processingThread = new Thread(this::processLoop, "WAL-Writer");
        this.processingThread.setDaemon(false); // Ensure writes complete before JVM shutdown
        this.processingThread.start();
    }

    public void offer(BatchItem batchItem) {
        queue.offer(batchItem);
    }

    public void shutdown() {
        running = false;
        processingThread.interrupt();
        try {
            processingThread.join(5000); // Wait up to 5 seconds for graceful shutdown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for WAL writer to shut down");
        }
    }

    private void processLoop() {
        while (running) {
            try {
                // Wait for at least one item
                BatchItem firstItem = queue.poll(100, TimeUnit.MILLISECONDS);
                if (firstItem == null) {
                    continue; // Timeout, check running flag and loop
                }

                List<BatchItem> operations = new ArrayList<>();
                operations.add(firstItem);

                // Drain additional items up to batch size for better throughput
                queue.drainTo(operations, MAX_BATCH_SIZE - 1);

                processBatch(operations);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("WAL writer interrupted, shutting down");
                break;
            }
        }

        // Process remaining items before shutdown to ensure consistency
        List<BatchItem> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            processBatch(remaining);
        }
    }

    private void processBatch(List<BatchItem> operations) {
        try {
            byte[] bytesToWrite = new byte[0];
            // TODO: optimize by using a ByteArrayOutputStream?
            for (BatchItem batchItem : operations) {
                bytesToWrite = Bytes.concat(
                        bytesToWrite, Operation.toBytes(batchItem.operation), WriteAheadLogger.SEPARATOR_BYTES);
            }
            Files.write(path, bytesToWrite, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            for (BatchItem batchItem : operations) {
                batchItem.future.complete(new BatchResult(null));
            }
            logger.info("Processed batch of {} WAL operations", operations.size());
        } catch (IOException e) {
            for (BatchItem batchItem : operations) {
                batchItem.future.complete(new BatchResult(e));
            }
            logger.warn("Failed to process batch of {} WAL operations", operations.size());
            throw new RuntimeException("Failed to write to WAL log", e);
        }
    }

    public record BatchItem(Operation operation, CompletableFuture<BatchResult> future) {}

    public record BatchResult(Exception exception) {}
}
