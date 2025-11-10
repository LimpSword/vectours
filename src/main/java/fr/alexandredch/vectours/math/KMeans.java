package fr.alexandredch.vectours.math;

import java.util.*;

public final class KMeans {

    private static final Random random = new Random();

    public static List<Cluster> fit(double[][] data) {
        int clusterCount = (int) Math.sqrt(data.length);

        // Create clusters with random centroids
        List<Cluster> clusters = generateRandomClusters(data, clusterCount);

        boolean converged = false;
        while (!converged) {
            Map<Cluster, List<double[]>> assignments = new HashMap<>();

            // Assign points to the closest cluster
            for (double[] vector : data) {
                Cluster closestCluster = null;
                double closestDistance = Float.MAX_VALUE;
                for (Cluster cluster : clusters) {
                    double distance = Vectors.euclideanDistance(vector, cluster.getCentroid());
                    if (distance < closestDistance) {
                        closestDistance = distance;
                        closestCluster = cluster;
                    }
                }
                assignments.computeIfAbsent(closestCluster, k -> new ArrayList<>()).add(vector);
            }

            // Recalculate centroids
            List<Cluster> newClusters = new ArrayList<>();
            for (Cluster cluster : clusters) {
                List<double[]> assignedPoints = assignments.get(cluster);
                if (assignedPoints == null || assignedPoints.isEmpty()) {
                    newClusters.add(cluster);
                    continue;
                }

                double[] newCentroid = new double[cluster.getCentroid().length];
                for (double[] point : assignedPoints) {
                    for (int i = 0; i < point.length; i++) {
                        newCentroid[i] += point[i];
                    }
                }
                for (int i = 0; i < newCentroid.length; i++) {
                    newCentroid[i] /= assignedPoints.size();
                }

                Cluster newCluster = new Cluster(newCentroid);
                newCluster.setData(assignedPoints.toArray(new double[0][]));
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

    private static List<Cluster> generateRandomClusters(double[][] data, int clusterCount) {
        List<Cluster> clusters = new ArrayList<>(clusterCount);
        for (int i = 0; i < clusterCount; i++) {
            random.ints(0, data.length).distinct().limit(clusterCount).forEach(index -> {
                clusters.add(new Cluster(data[index]));
            });
        }
        return clusters;
    }
}
