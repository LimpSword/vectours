package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.serialization.InMemorySerializer;
import fr.alexandredch.vectours.store.Store;
import fr.alexandredch.vectours.store.background.SegmentSaverTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class InMemoryStore implements Store {

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentSaverTask segmentSaverTask;

    private final List<Segment> segments = new ArrayList<>();

    private Segment currentSegment;

    public InMemoryStore() {
        writeAheadLogger = new WriteAheadLogger();
        segmentSaverTask = new SegmentSaverTask(writeAheadLogger);

        // TODO: Recover non-saved segments from WAL
        currentSegment = new Segment(writeAheadLogger.getLatestSegmentIdIncludingUnclosed() + 1);

        scheduledExecutorService.scheduleAtFixedRate(segmentSaverTask, 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void initFromDisk() {
        // Read all segments from disk

        // Replay WAL from last checkpoint
    }

    @Override
    public void insert(String id, Vector vector) {
        if (currentSegment.isFull()) {
            segments.add(currentSegment);

            int newSegmentId = currentSegment.getId() + 1;

            // Submit segment to background saver
            segmentSaverTask.submitSegment(currentSegment);

            // Create new segment and log it
            currentSegment = new Segment(newSegmentId);
            writeAheadLogger.newSegment(currentSegment);
        }
        try {
            // Append to WAL
            byte[] segmentData = InMemorySerializer.serialize(vector.values());
            writeAheadLogger.applyOperation(new Operation.Insert(id, segmentData));

            // Add to segment
            currentSegment.insert(vector);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SearchResult> search(float[] vector, int k) {
        // TODO: Search in all segments and merge results

        /*TreeMap<Double, Vector> map = new TreeMap<>();
        for (Map.Entry<String, Vector> entry : store.entrySet()) {
            double distance = Vectors.euclideanDistance(vector, entry.getValue().values());
            map.put(distance, entry.getValue());
        }

        return map.entrySet().stream()
                .limit(k)
                .map(e -> new SearchResult(
                        e.getValue().id(), e.getKey(), e.getValue().metadata()))
                .toList();*/
        return List.of();
    }

    @Override
    public void delete(String id) {
        // Append to WAL
        writeAheadLogger.applyOperation(new Operation.Delete(id));

        // Delete from its segment
        for (Segment segment : segments) {
            if (segment.containsId(id)) {
                segment.delete(id);
                return;
            }
        }
        if (currentSegment.containsId(id)) {
            currentSegment.delete(id);
        }
    }

    @Override
    public Vector getVector(String id) {
        for (Segment segment : segments) {
            if (segment.containsId(id)) {
                return segment.getVector(id);
            }
        }
        if (currentSegment.containsId(id)) {
            return currentSegment.getVector(id);
        }
        return null;
    }

    @Override
    public void dropAll() {
        segments.clear();
        currentSegment = new Segment(0);
        writeAheadLogger.clearLog();
    }
}
