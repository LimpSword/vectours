package fr.alexandredch.vectours.index;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Cluster;
import fr.alexandredch.vectours.math.KMeans;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.store.base.SegmentStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class IVFIndex {

    public static final int MIN_VECTORS_FOR_IVF_INDEX = 10_000;

    private final List<Cluster> clusters;
    private final SegmentStore segmentStore;
    private boolean built = false;

    public IVFIndex(SegmentStore segmentStore) {
        this.segmentStore = segmentStore;
        // TODO: Clusters should save the same Vector objects as in the segment store (not copies)
        List<Vector> vectors = segmentStore.getAllVectors();
        if (vectors.size() <= MIN_VECTORS_FOR_IVF_INDEX) {
            this.clusters = new ArrayList<>();
            return;
        }
        this.clusters = KMeans.fit(vectors);
        this.built = true;
    }

    public boolean canSearch() {
        return this.built;
    }

    public void insertVector(Vector vector) {
        // Add to the closest cluster or rebuild the index
        if (this.built) {
            Cluster closestCluster = findClosestClusters(vector.values(), 1).getFirst();
            closestCluster.addVector(vector);
        } else if (segmentStore.getAllVectors().size() > MIN_VECTORS_FOR_IVF_INDEX) {
            // Rebuild the index
            List<Vector> vectors = segmentStore.getAllVectors();
            this.clusters.clear();
            this.clusters.addAll(KMeans.fit(vectors));
            this.built = true;
        }
    }

    public List<Vector> search(double[] vector, int nprobe) {
        List<Vector> result = new ArrayList<>();
        System.out.println("yo");

        // Find the nprobe closest clusters
        List<Cluster> closestClusters = findClosestClusters(vector, nprobe);
        for (Cluster cluster : closestClusters) {
            result.addAll(searchInCluster(cluster, vector, nprobe));
        }

        return result.stream()
                .sorted(Comparator.comparingDouble(v -> Vectors.euclideanDistance(v.values(), vector)))
                .limit(nprobe)
                .toList();
    }

    private List<Vector> searchInCluster(Cluster cluster, double[] vector, int nprobe) {
        return cluster.getData().stream()
                .sorted(Comparator.comparingDouble(v -> Vectors.euclideanDistance(v.values(), vector)))
                .limit(nprobe)
                .toList();
    }

    private List<Cluster> findClosestClusters(double[] vector, int nprobe) {
        return clusters.stream()
                .sorted(Comparator.comparingDouble(c -> Vectors.euclideanDistance(c.getCentroid(), vector)))
                .limit(nprobe)
                .toList();
    }
}
