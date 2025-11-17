package fr.alexandredch.vectours.index.ivf;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Cluster;
import fr.alexandredch.vectours.math.KMeans;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class DefaultIVFIndex implements IVFIndex {

    // TODO: we need an index per dimension

    public static final int MIN_VECTORS_FOR_IVF_INDEX = 10_000;

    private final List<Cluster> clusters;
    private final SegmentStore segmentStore;
    private boolean built = false;

    public DefaultIVFIndex(SegmentStore segmentStore) {
        this.segmentStore = segmentStore;
        List<Vector> vectors = segmentStore.getAllVectors();
        if (vectors.size() <= MIN_VECTORS_FOR_IVF_INDEX) {
            this.clusters = new ArrayList<>();
            return;
        }
        this.clusters = KMeans.fit(vectors);
        this.built = true;
    }

    @Override
    public boolean canSearch() {
        return this.built;
    }

    @Override
    public void insertVector(Vector vector) {
        // Add to the closest cluster or rebuild the index
        if (this.built) {
            Cluster closestCluster =
                    findClosestClusters(vector.values(), 1).findFirst().orElseThrow();
            closestCluster.addVector(vector);
        } else if (segmentStore.getTotalVectorCount() > MIN_VECTORS_FOR_IVF_INDEX) {
            // Rebuild the index
            List<Vector> vectors = segmentStore.getAllVectors();
            this.clusters.clear();
            this.clusters.addAll(KMeans.fit(vectors));
            this.built = true;
        }
    }

    @Override
    public List<Vector> search(double[] vector, int nprobe) {
        List<Vector> result = new ArrayList<>();

        // Find the nprobe closest clusters
        findClosestClusters(vector, nprobe).forEach(cluster -> {
            result.addAll(searchInCluster(cluster, vector, nprobe));
        });

        return result.stream()
                .sorted(Comparator.comparingDouble(v -> Vectors.squaredEuclidianDistance(v.values(), vector)))
                .limit(nprobe)
                .toList();
    }

    private List<Vector> searchInCluster(Cluster cluster, double[] vector, int nprobe) {
        return cluster.getData().stream()
                .sorted(Comparator.comparingDouble(v -> Vectors.squaredEuclidianDistance(v.values(), vector)))
                .limit(nprobe)
                .toList();
    }

    private Stream<Cluster> findClosestClusters(double[] vector, int nprobe) {
        return clusters.stream()
                .sorted(Comparator.comparingDouble(c -> Vectors.squaredEuclidianDistance(c.getCentroid(), vector)))
                .limit(nprobe);
    }
}
