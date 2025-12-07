package fr.alexandredch.vectours.store.background;

import fr.alexandredch.vectours.store.segment.Segment;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import fr.alexandredch.vectours.store.wal.WriteAheadLogger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SegmentSaverTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SegmentSaverTask.class);

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;
    private final Lock lock = new ReentrantLock();

    public SegmentSaverTask(WriteAheadLogger writeAheadLogger, SegmentStore segmentStore) {
        this.writeAheadLogger = writeAheadLogger;
        this.segmentStore = segmentStore;
    }

    @Override
    public void run() {
        saveSegments();
    }

    public void saveSegments() {
        // Don't save segments if another thread is already saving
        if (lock.tryLock()) {
            logger.info(
                    "Saving {}/{} dirty segments to disk.",
                    segmentStore.getSegments().stream().filter(Segment::isDirty).count(),
                    segmentStore.getSegments().size());
            segmentStore.getSegments().stream().filter(Segment::isDirty).forEach(segment -> {
                // Save segment to disk
                segmentStore.saveSegmentToDisk(segment);

                // Mark segment as clean
                segment.setDirty(false);

                // Move WAL checkpoint
                writeAheadLogger.markLastCheckpoint(segment);
            });
            logger.info("Segments saved successfully.");
            lock.unlock();
        }
    }
}
