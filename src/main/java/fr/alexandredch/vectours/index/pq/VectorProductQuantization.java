package fr.alexandredch.vectours.index.pq;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.ivf.QuantizedIVFIndex;
import fr.alexandredch.vectours.math.Vectors;
import java.util.Arrays;
import java.util.List;

public final class VectorProductQuantization {

    private final QuantizedIVFIndex quantizedIVFIndex;

    public VectorProductQuantization(QuantizedIVFIndex quantizedIVFIndex) {
        this.quantizedIVFIndex = quantizedIVFIndex;
    }

    public Vector quantizeVector(Vector vector) {
        int dimension = vector.values().length;
        int numberOfSubVectors = getNumberOfSubVectors(dimension);

        double[] quantizedVector = new double[numberOfSubVectors];

        for (int i = 0; i < dimension; i += numberOfSubVectors) {
            int subVectorSize = Math.min(numberOfSubVectors, dimension - i);
            double[] subVector = Arrays.copyOfRange(vector.values(), i, i + subVectorSize);
            int index = Math.divideExact(i, numberOfSubVectors);

            // Find the closest sub-vector (centroid)
            quantizedVector[index] = findClosestCentroid(subVector, index);
        }

        return new Vector(vector.id(), quantizedVector, vector.metadata());
    }

    private double findClosestCentroid(double[] subVector, int index) {
        List<double[]> centroids = quantizedIVFIndex.getCentroids(index);
        double minDistance = Double.MAX_VALUE;
        int centroidIndex = 0;
        for (int i = 0; i < centroids.size(); i++) {
            double distance = Vectors.squaredEuclidianDistance(subVector, centroids.get(i));
            if (distance < minDistance) {
                minDistance = distance;
                centroidIndex = i;
            }
        }
        return centroids.get(centroidIndex)[0];
    }

    private int getNumberOfSubVectors(int dimension) {
        return Math.max(2, (int) Math.ceil(Math.sqrt(dimension)));
    }
}
