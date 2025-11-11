package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.IVFIndex;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.store.Store;
import fr.alexandredch.vectours.store.background.SegmentSaverTask;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class InMemoryStore implements Store {

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;
    private final SegmentSaverTask segmentSaverTask;

    private IVFIndex ivfIndex;

    public InMemoryStore() {
        writeAheadLogger = new WriteAheadLogger();
        segmentStore = new SegmentStore(writeAheadLogger);
        segmentSaverTask = new SegmentSaverTask(writeAheadLogger, segmentStore);

        scheduledExecutorService.scheduleAtFixedRate(segmentSaverTask, 0, 30, TimeUnit.SECONDS);
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
                case Operation.Insert insert -> {
                    segmentStore.insertVector(insert.vector());
                }
                case Operation.Delete delete -> {
                    segmentStore.deleteVector(delete.id());
                }
            }
        }

        ivfIndex = new IVFIndex(segmentStore);
    }

    @Override
    public void insert(Vector vector) {
        // Append to WAL
        writeAheadLogger.applyOperation(new Operation.Insert(vector));

        // Add to segment
        segmentStore.insertVector(vector);
        ivfIndex.insertVector(vector);
    }

    @Override
    public List<SearchResult> search(double[] searchedVector, int k) {
        if (ivfIndex.canSearch()) {
            return ivfIndex.search(searchedVector, k).stream()
                    .map(v -> new SearchResult(
                            v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                    .toList();
        }
        return segmentStore.getAllVectors().stream()
                .map(v -> new SearchResult(
                        v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                .sorted(Comparator.comparingDouble(SearchResult::distance))
                .limit(k)
                .toList();
    }

    @Override
    public List<SearchResult> search(SearchParameters searchParameters) {
        double[] searchedVector = searchParameters.searchedVector();
        if (searchParameters.allowIVF() && ivfIndex.canSearch()) {
            return ivfIndex.search(searchedVector, searchParameters.topK()).stream()
                    .map(v -> new SearchResult(
                            v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                    .toList();
        }
        return segmentStore.getAllVectors().stream()
                .map(v -> new SearchResult(
                        v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                .sorted(Comparator.comparingDouble(SearchResult::distance))
                .limit(searchParameters.topK())
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
    }

    public void shutdown() {
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void saveAll() {
        segmentSaverTask.saveSegments();
    }
}
