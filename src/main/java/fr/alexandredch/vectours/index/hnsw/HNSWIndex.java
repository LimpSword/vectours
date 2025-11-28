package fr.alexandredch.vectours.index.hnsw;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.*;

/**
 * HNSWIndex
 * <p>
 * Implementation of the HNSW (Hierarchical Navigable Small World) algorithm for approximate nearest neighbor search.
 * Each vector is represented as a node in a multi-layer graph, where edges connect nodes based on their proximity.
 */
public final class HNSWIndex {

    private static final int MAX_LAYER = 16;
    private static final int M = 16;
    private static final int EF_CONSTRUCTION = 200;
    private static final int EF_SEARCH = 50;

    private final SegmentStore segmentStore;

    // Multi-layer graph: layer -> node (vector id) -> list of neighbor IDs
    private final List<Map<String, Set<String>>> layers = new ArrayList<>();

    private String entryPointId;

    public HNSWIndex(SegmentStore segmentStore) {
        this.segmentStore = segmentStore;

        for (int i = 0; i <= MAX_LAYER; i++) {
            layers.add(new HashMap<>());
        }
    }

    public List<Vector> search(double[] vector, int nprobe) {
        return search(vector, nprobe, EF_SEARCH);
    }

    public List<Vector> search(double[] vector, int nprobe, int efSearch) {
        return null;
    }

    public void insertVector(Vector vector) {
        int layer = randomLayer();

        // Add the node to all layers up to the selected layer
        for (int i = 0; i <= layer; i++) {
            layers.get(i).put(vector.id(), Set.of());
        }

        if (entryPointId == null) {
            entryPointId = vector.id();
            return;
        }

        List<String> currentEntryPointId = List.of(entryPointId);

        // Find the nearest neighbor in the selected layer
        for (int l = MAX_LAYER; l > layer; l--) {
            currentEntryPointId = getNeighbors(vector, currentEntryPointId, 1, l);
        }

        // Insert the node at each layer from the selected layer down to the entry point layer
        for (int l = layer; l >= 0; l--) {
            List<String> neighbors = getNeighbors(vector, currentEntryPointId, EF_CONSTRUCTION, l);

            // Select M neighbors (temp)
            List<String> selectedNeighbors = neighbors.subList(0, Math.min(M, neighbors.size()));

            for (String neighborId : selectedNeighbors) {
                layers.get(l).get(neighborId).add(vector.id());
                layers.get(l).get(vector.id()).add(neighborId);

                if (layers.get(l).get(neighborId).size() > M) {
                    // Prune the list of neighbors
                }
            }
            currentEntryPointId = selectedNeighbors;
        }
    }

    private List<String> getNeighbors(Vector vector, List<String> entrypoints, int count, int layer) {
        return List.of();
    }

    private int randomLayer() {
        int level = 0;
        double random = Math.random();
        while (random < 1. / Math.exp(1) && level < MAX_LAYER) {
            level++;
        }
        return level;
    }
}
