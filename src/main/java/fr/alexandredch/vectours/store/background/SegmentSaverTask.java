package fr.alexandredch.vectours.store.background;

import fr.alexandredch.vectours.store.base.Segment;
import fr.alexandredch.vectours.store.base.SegmentStore;
import fr.alexandredch.vectours.store.base.WriteAheadLogger;

public final class SegmentSaverTask implements Runnable {

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;

    public SegmentSaverTask(WriteAheadLogger writeAheadLogger, SegmentStore segmentStore) {
        this.writeAheadLogger = writeAheadLogger;
        this.segmentStore = segmentStore;
    }

    @Override
    public void run() {
        saveSegments();
    }

    public void saveSegments() {
        segmentStore.getSegments().stream().filter(Segment::isDirty).forEach(segment -> {
            // Save segment to disk
            segmentStore.saveSegmentToDisk(segment);

            // Mark segment as clean
            segment.setDirty(false);

            // Move WAL checkpoint
            writeAheadLogger.markLastCheckpoint(segment);
        });
    }
}
