package fr.alexandredch.vectours.store.background;

import fr.alexandredch.vectours.store.base.Segment;
import fr.alexandredch.vectours.store.base.SegmentStore;
import fr.alexandredch.vectours.store.base.WriteAheadLogger;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SegmentSaverTask implements Runnable {

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;

    private final Queue<Segment> queue = new ConcurrentLinkedQueue<>();

    public SegmentSaverTask(WriteAheadLogger writeAheadLogger, SegmentStore segmentStore) {
        this.writeAheadLogger = writeAheadLogger;
        this.segmentStore = segmentStore;
    }

    public void submitSegment(Segment segment) {
        queue.add(segment);
    }

    @Override
    public void run() {
        Segment segment = queue.poll();
        if (segment != null) {
            // Save segment to disk
            segmentStore.saveSegmentToDisk(segment);

            // Close segment after saving
            segment.close();

            // Move WAL checkpoint
            writeAheadLogger.markLastCheckpoint(segment);
        }
    }
}
