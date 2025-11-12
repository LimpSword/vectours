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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WALWriterBatcher {

    private static final Logger logger = LoggerFactory.getLogger(WALWriterBatcher.class);

    private static final int MAX_BATCH_SIZE = 500;
    private static final Path path = Paths.get(WriteAheadLogger.LOG_FILE_NAME);

    private final BlockingQueue<BatchItem> queue = new LinkedBlockingQueue<>();
    private final Lock lock = new ReentrantLock();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public void offer(BatchItem batchItem) {
        queue.offer(batchItem);
        process();
    }

    private void process() {
        // TODO: optimize by having a single thread processing batches with an atomic flag?
        scheduler.execute(() -> {
            lock.lock();
            List<BatchItem> operations = new ArrayList<>();
            try {
                queue.drainTo(operations, MAX_BATCH_SIZE);
                if (operations.isEmpty()) {
                    return;
                }

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
            } finally {
                lock.unlock();
            }
        });
    }

    public record BatchItem(Operation operation, CompletableFuture<BatchResult> future) {}

    public record BatchResult(Exception exception) {}
}
