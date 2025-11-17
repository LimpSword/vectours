package fr.alexandredch.vectours.index.ivf;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.pq.VectorProductQuantization;
import fr.alexandredch.vectours.math.Cluster;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class QuantizedIVFIndex implements IVFIndex {

    public static final int MIN_VECTORS_FOR_IVF_INDEX = 10_000;

    private final SegmentStore segmentStore;
    private final VectorProductQuantization productQuantization;

    private final List<Cluster<String>> clusters = new ArrayList<>();
    private boolean built = false;

    public QuantizedIVFIndex(SegmentStore segmentStore) {
        this.segmentStore = segmentStore;
        this.productQuantization = new VectorProductQuantization(this);
        List<Vector> vectors = segmentStore.getAllVectors();
        if (vectors.size() <= MIN_VECTORS_FOR_IVF_INDEX) {
            return;
        }
        buildCentroids();
        this.built = true;
    }

    @Override
    public boolean canSearch() {
        return this.built;
    }

    @Override
    public void insertVector(Vector vector) {
        if (this.built) {
            // Add to corresponding centroid
            Vector quantizedVector = productQuantization.quantizeVector(vector);
            for (Cluster<String> cluster : clusters) {
                if (Arrays.equals(cluster.getCentroid(), quantizedVector.values())) {
                    cluster.add(vector.id());
                    return;
                }
            }
        } else if (segmentStore.getTotalVectorCount() > MIN_VECTORS_FOR_IVF_INDEX) {
            // Rebuild the index
            this.clusters.clear();
            buildCentroids();
            this.built = true;
        }
    }

    @Override
    public List<Vector> search(double[] vector, int nprobe) {
        return List.of();
    }

    public List<double[]> getCentroids(int index) {
        return clusters.stream().map(Cluster::getCentroid).toList();
    }

    private void buildCentroids() {
        List<Vector> vectors = segmentStore.getAllVectors();
    }
}
