package fr.alexandredch.vectours.math;

import fr.alexandredch.vectours.data.Vector;
import java.util.*;

public final class KMeans {

    private static final int MAX_ITERATIONS = 50;
    private static final double tolerance = 1e-4;

    private static final Random random = new Random();

    public static List<Cluster<Vector>> fit(List<Vector> data) {
        int clusterCount = (int) (Math.log(data.size()) * 3);
        return fit(data, clusterCount);
    }

    public static List<Cluster<Vector>> fit(List<Vector> data, int clusterCount) {
        // Create clusters with random centroids
        List<Cluster<Vector>> clusters = generateRandomClusters(data, clusterCount);

        boolean converged = false;

        int iterations = 0;

        while (!converged && iterations < MAX_ITERATIONS) {
            iterations++;
            Map<Cluster<Vector>, List<Vector>> assignments = new HashMap<>();

            // Assign points to the closest cluster
            for (Vector vector : data) {
                Cluster<Vector> closestCluster = null;
                double closestDistance = Float.MAX_VALUE;
                for (Cluster<Vector> cluster : clusters) {
                    double distance = Vectors.squaredEuclidianDistance(vector.values(), cluster.getCentroid());
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
            List<Cluster<Vector>> newClusters = new ArrayList<>();
            for (Cluster<Vector> cluster : clusters) {
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

                Cluster<Vector> newCluster = new Cluster<>(newCentroid);
                newCluster.setData(assignedPoints);
                newClusters.add(newCluster);
            }

            // Check for convergence
            converged = hasClusterConverged(clusters, newClusters);

            clusters = newClusters;
        }

        return clusters;
    }

    private static boolean hasClusterConverged(List<Cluster<Vector>> clusters, List<Cluster<Vector>> newClusters) {
        boolean converged = true;
        for (int i = 0; i < clusters.size(); i++) {
            double distance = Vectors.squaredEuclidianDistance(
                    clusters.get(i).getCentroid(), newClusters.get(i).getCentroid());
            if (distance > tolerance) {
                converged = false;
            }
        }
        return converged;
    }

    private static List<Cluster<Vector>> generateRandomClusters(List<Vector> data, int clusterCount) {
        List<Cluster<Vector>> clusters = new ArrayList<>(clusterCount);
        random.ints(0, data.size())
                .distinct()
                .limit(clusterCount)
                .forEach(index -> clusters.add(new Cluster<>(data.get(index).values())));
        return clusters;
    }
}
