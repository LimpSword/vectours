package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Metadata;
import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.serialization.InMemorySerializer;
import fr.alexandredch.vectours.store.Store;
import fr.alexandredch.vectours.store.background.SegmentSaverTask;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class InMemoryStore implements Store {

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;

    public InMemoryStore() {
        writeAheadLogger = new WriteAheadLogger();
        segmentStore = new SegmentStore(writeAheadLogger);
    }

    public void runTasks() {
        // Interferes with tests, so not running it by default
        SegmentSaverTask segmentSaverTask = new SegmentSaverTask(writeAheadLogger, segmentStore);
        scheduledExecutorService.scheduleAtFixedRate(segmentSaverTask, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void initFromDisk() {
        // Read all segments from disk
        segmentStore.loadFromDisk();

        // Replay WAL from last checkpoint
        // TODO: improve this by keeping the segments as defined in the WAL
        List<Operation> operations = writeAheadLogger.loadFromCheckpoint();
        for (Operation operation : operations) {
            switch (operation) {
                // TODO: we should have a single method to apply operations to avoid code duplication
                case Operation.Insert insert -> {
                    try {
                        double[] values = InMemorySerializer.deserialize(insert.vectorBytes());
                        // TODO: handle metadata
                        Vector vector = new Vector(insert.id(), values, new Metadata(Map.of()));
                        segmentStore.insertVector(vector);
                    } catch (IOException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                case Operation.Delete delete -> {
                    segmentStore.deleteVector(delete.id());
                }
            }
        }
    }

    @Override
    public void insert(String id, Vector vector) {
        try {
            // Append to WAL
            byte[] segmentData = InMemorySerializer.serialize(vector.values());
            writeAheadLogger.applyOperation(new Operation.Insert(id, segmentData));

            // Add to segment
            segmentStore.insertVector(vector);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SearchResult> search(double[] searchedVector, int k) {
        TreeMap<Double, Vector> map = new TreeMap<>();
        for (Vector vector : segmentStore.getAllVectors()) {
            double distance = Vectors.euclideanDistance(searchedVector, vector.values());
            map.put(distance, vector);
        }

        return map.entrySet().stream()
                .limit(k)
                .map(e -> new SearchResult(
                        e.getValue().id(), e.getKey(), e.getValue().metadata()))
                .toList();
    }

    @Override
    public void delete(String id) {
        // Append to WAL
        writeAheadLogger.applyOperation(new Operation.Delete(id));

        // Delete from its segment
        segmentStore.deleteVector(id);
    }

    @Override
    public Vector getVector(String id) {
        return segmentStore.getVectorById(id);
    }

    @Override
    public void dropAll() {
        segmentStore.close();
        writeAheadLogger.clearLog();

        scheduledExecutorService.shutdownNow();
    }
}
