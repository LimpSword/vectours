package fr.alexandredch.vectours.store.background;

import fr.alexandredch.vectours.store.base.Segment;
import fr.alexandredch.vectours.store.base.WriteAheadLogger;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SegmentSaverTask implements Runnable {

    private final WriteAheadLogger writeAheadLogger;

    private final Queue<Segment> queue = new ConcurrentLinkedQueue<>();

    public SegmentSaverTask(WriteAheadLogger writeAheadLogger) {
        this.writeAheadLogger = writeAheadLogger;
    }

    public void submitSegment(Segment segment) {
        queue.add(segment);
    }

    @Override
    public void run() {
        Segment segment = queue.poll();
        if (segment != null) {
            // Save segment to disk
            saveSegmentToDisk(segment);

            // Close segment after saving
            segment.close();

            // Move WAL checkpoint
            writeAheadLogger.markLastCheckpoint(segment);
        }
    }

    private void saveSegmentToDisk(Segment segment) {}
}
