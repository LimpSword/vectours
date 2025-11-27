package fr.alexandredch.vectours.index.hnsw;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.List;

public final class HNSWIndex {

    public static final int MIN_VECTORS_FOR_HNSW_INDEX = 10_000;

    private static final int M = 16;
    private static final int EF_CONSTRUCTION = 200;
    private static final int EF_SEARCH = 50;

    private final SegmentStore segmentStore;

    private boolean built = false;

    public HNSWIndex(SegmentStore segmentStore) {
        this.segmentStore = segmentStore;
        if (segmentStore.getTotalVectorCount() > MIN_VECTORS_FOR_HNSW_INDEX) {
            buildIndex();
            this.built = true;
        }
    }

    public boolean canSearch() {
        return this.built;
    }

    public List<Vector> search(double[] vector, int nprobe) {
        return search(vector, nprobe, EF_SEARCH);
    }

    public List<Vector> search(double[] vector, int nprobe, int efSearch) {
        return null;
    }

    public void insertVector(Vector vector) {}

    private void buildIndex() {}
}
