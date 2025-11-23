package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.ivf.DefaultIVFIndex;
import fr.alexandredch.vectours.index.pq.VectorProductQuantization;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.store.Store;
import fr.alexandredch.vectours.store.background.SegmentSaverTask;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import fr.alexandredch.vectours.store.wal.WriteAheadLogger;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InMemoryStore implements Store {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final WriteAheadLogger writeAheadLogger;
    private final SegmentStore segmentStore;
    private final SegmentSaverTask segmentSaverTask;
    private final VectorProductQuantization vectorProductQuantization;

    private DefaultIVFIndex defaultIvfIndex;

    public InMemoryStore() {
        writeAheadLogger = new WriteAheadLogger();
        segmentStore = new SegmentStore(writeAheadLogger);
        segmentSaverTask = new SegmentSaverTask(writeAheadLogger, segmentStore);
        // TODO: create 1 per dimension
        vectorProductQuantization = new VectorProductQuantization(segmentStore, 0);

        scheduledExecutorService.scheduleAtFixedRate(segmentSaverTask, 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void initFromDisk() {
        logger.info("Initializing InMemoryStore from disk...");

        logger.info("Reading segments from disk...");
        // Read all segments from disk
        segmentStore.loadFromDisk();

        // Replay WAL from last checkpoint
        logger.info("Loading WAL operations from last checkpoint...");
        List<Operation> operations = writeAheadLogger.loadFromCheckpoint();
        for (Operation operation : operations) {
            switch (operation) {
                case Operation.CreateSegment createSegment -> {
                    segmentStore.createSegmentIfNotExists(createSegment.segmentId(), true);
                }
                case Operation.Insert insert -> {
                    segmentStore.insertVector(insert.vector());
                }
                case Operation.InsertInSegment insertInSegment -> {
                    segmentStore.insertVectorInSegment(insertInSegment.vector(), insertInSegment.segmentId());
                }
                case Operation.Delete delete -> {
                    segmentStore.deleteVector(delete.id());
                }
            }
        }

        logger.info("Creating IVF index...");
        defaultIvfIndex = new DefaultIVFIndex(segmentStore);
        logger.info("Finished initializing InMemoryStore from disk.");

        logger.info("Building PQ centroids...");
        vectorProductQuantization.buildSubspaces();
        logger.info("Finished building PQ centroids.");
    }

    @Override
    public CompletableFuture<Void> insert(Vector vector) {
        // Append to WAL
        return writeAheadLogger.applyOperation(new Operation.Insert(vector)).thenRun(() -> {
            // Insert into its segment and IVF index
            segmentStore.insertVector(vector);
            defaultIvfIndex.insertVector(vector);

            vectorProductQuantization.insertVector(vector);
            vectorProductQuantization.buildSubspaces();
        });
    }

    @Override
    public List<SearchResult> search(double[] searchedVector, int k) {
        return search(new SearchParameters.Builder()
                .searchedVector(searchedVector)
                .topK(k)
                .build());
    }

    @Override
    public List<SearchResult> search(SearchParameters searchParameters) {
        double[] searchedVector = searchParameters.searchedVector();
        if (searchParameters.allowIVF() && defaultIvfIndex.canSearch()) {
            return defaultIvfIndex.search(searchedVector, searchParameters.topK()).stream()
                    .map(v -> new SearchResult(
                            v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                    .toList();
        }
        if (searchParameters.usePQ()) {
            return vectorProductQuantization.approxSearch(searchedVector, searchParameters.topK());
        }
        return segmentStore.getAllVectors().stream()
                .map(v -> new SearchResult(
                        v.id(), Vectors.squaredEuclidianDistance(v.values(), searchedVector), v.metadata()))
                .sorted(Comparator.comparingDouble(SearchResult::distance))
                .limit(searchParameters.topK())
                .toList();
    }

    @Override
    public CompletableFuture<Void> delete(String id) {
        // Append to WAL
        return writeAheadLogger.applyOperation(new Operation.Delete(id)).thenRun(() -> {
            // Delete from its segment
            segmentStore.deleteVector(id);
        });
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
        writeAheadLogger.shutdown();
    }

    @Override
    public void saveAll() {
        segmentSaverTask.saveSegments();
    }
}
