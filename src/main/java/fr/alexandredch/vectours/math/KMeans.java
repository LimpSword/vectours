package fr.alexandredch.vectours.math;

import fr.alexandredch.vectours.data.Vector;
import java.util.*;

public final class KMeans {

    private static final Random random = new Random();

    public static List<Cluster> fit(List<Vector> data) {
        int clusterCount = (int) Math.sqrt(data.size());

        // Create clusters with random centroids
        List<Cluster> clusters = generateRandomClusters(data, clusterCount);

        boolean converged = false;
        while (!converged) {
            Map<Cluster, List<Vector>> assignments = new HashMap<>();

            // Assign points to the closest cluster
            for (Vector vector : data) {
                Cluster closestCluster = null;
                double closestDistance = Float.MAX_VALUE;
                for (Cluster cluster : clusters) {
                    double distance = Vectors.euclideanDistance(vector.values(), cluster.getCentroid());
                    if (distance < closestDistance) {
                        closestDistance = distance;
                        closestCluster = cluster;
                    }
                }
                assignments
                        .computeIfAbsent(closestCluster, _cluster -> new ArrayList<>())
                        .add(vector);
            }

            // Recalculate centroids
            List<Cluster> newClusters = new ArrayList<>();
            for (Cluster cluster : clusters) {
                List<Vector> assignedPoints = assignments.get(cluster);
                if (assignedPoints == null || assignedPoints.isEmpty()) {
                    newClusters.add(cluster);
                    continue;
                }

                double[] newCentroid = new double[cluster.getCentroid().length];
                for (Vector point : assignedPoints) {
                    for (int i = 0; i < point.values().length; i++) {
                        newCentroid[i] += point.values()[i];
                    }
                }
                for (int i = 0; i < newCentroid.length; i++) {
                    newCentroid[i] /= assignedPoints.size();
                }

                Cluster newCluster = new Cluster(newCentroid);
                newCluster.setData(assignedPoints);
                newClusters.add(newCluster);
            }

            // Check for convergence
            if (newClusters.equals(clusters)) {
                converged = true;
            }

            clusters = newClusters;
        }

        return clusters;
    }

    private static List<Cluster> generateRandomClusters(List<Vector> data, int clusterCount) {
        List<Cluster> clusters = new ArrayList<>(clusterCount);
        random.ints(0, data.size())
                .distinct()
                .limit(clusterCount)
                .forEach(index -> clusters.add(new Cluster(data.get(index).values())));
        return clusters;
    }
}
