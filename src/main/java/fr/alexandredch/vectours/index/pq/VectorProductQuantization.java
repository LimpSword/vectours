package fr.alexandredch.vectours.index.pq;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Cluster;
import fr.alexandredch.vectours.math.KMeans;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VectorProductQuantization {

    private static final int MIN_VECTORS_FOR_PRODUCT_QUANTIZATION = 10_000;
    private static final int DEFAULT_CENTROIDS_PER_SUBSPACE = 256;

    private static final Logger logger = LoggerFactory.getLogger(VectorProductQuantization.class);

    private final int subSpacesCount;
    private final int centroidsPerSubSpaceCount;
    private final int subvectorDim; // Dimension of each subspace

    // codebooks[m] = centroids for subspace m
    // codebooks[m][k] = k-th centroid in subspace m (array of size subvectorDim)
    private double[][][] codebooks;

    // Store encoded vectors: vectorId -> byte array of codes
    private final Map<String, byte[]> encodedVectors;
    private final SegmentStore segmentStore;

    private boolean built = false;

    public VectorProductQuantization(SegmentStore segmentStore, int dimension) {
        this.segmentStore = segmentStore;
        this.subSpacesCount = calculateOptimalSubSpacesCount(dimension);
        this.centroidsPerSubSpaceCount = DEFAULT_CENTROIDS_PER_SUBSPACE;
        this.subvectorDim = dimension / subSpacesCount;
        this.encodedVectors = new HashMap<>();

        if (dimension % subSpacesCount != 0) {
            logger.error("Dimension {} is not divisible by subspaces count {}", dimension, subSpacesCount);
        }
    }

    public void insertVector(Vector vector) {
        if (!built) {
            // Index will be built later and the vector encoded at that time
            return;
        }
        byte[] codes = encode(vector.values());
        encodedVectors.put(vector.id(), codes);
    }

    public void buildSubspaces() {
        List<Vector> vectors = segmentStore.getAllVectors();

        if (vectors.size() < MIN_VECTORS_FOR_PRODUCT_QUANTIZATION) {
            logger.debug("Not enough vectors to build subspaces, skipping");
            return;
        }
        if (built) {
            // Just insert the new vector
            return;
        }

        logger.debug("Found {} vectors to build subspaces", vectors.size());
        codebooks = new double[subSpacesCount][centroidsPerSubSpaceCount][subvectorDim];

        // For each subspace
        for (int m = 0; m < subSpacesCount; m++) {
            logger.debug("Building subspace {}...", m);

            // Extract all subvectors for this subspace from all training vectors
            List<Vector> subvectors = new ArrayList<>();
            for (int i = 0; i < vectors.size(); i++) {
                Vector vector = vectors.get(i);
                double[] subvector = extractSubvector(vector.values(), m);
                subvectors.add(new Vector("sub_" + i, subvector, null));
            }

            // Run k-means on these subvectors to get K centroids
            List<Cluster<Vector>> clusters = KMeans.fit(subvectors, centroidsPerSubSpaceCount);

            for (int k = 0; k < centroidsPerSubSpaceCount; k++) {
                codebooks[m][k] = clusters.get(k).getCentroid();
            }
        }

        // Encode all vectors
        logger.debug("Encoding vectors...");
        for (Vector vector : vectors) {
            byte[] codes = encode(vector.values());
            encodedVectors.put(vector.id(), codes);
        }

        logger.info("Subspaces built");
        built = true;
    }

    public List<SearchResult> approxSearch(double[] query, int nprobe) {
        if (codebooks == null || encodedVectors.isEmpty()) {
            throw new IllegalStateException("Index not built. Call buildSubspaces() first.");
        }

        double[][] distanceTable = buildDistanceTable(query);

        return encodedVectors.entrySet().stream()
                .map(entry ->
                        new SearchResult(entry.getKey(), asymmetricDistance(distanceTable, entry.getValue()), null))
                .sorted(Comparator.comparingDouble(SearchResult::distance))
                .limit(nprobe)
                .toList();
    }

    /**
     * distanceTable[m][k] = ||query_subvector_m - codebook[m][k]||^2
     */
    private double[][] buildDistanceTable(double[] query) {
        double[][] distanceTable = new double[subSpacesCount][centroidsPerSubSpaceCount];

        for (int m = 0; m < subSpacesCount; m++) {
            double[] querySubvector = extractSubvector(query, m);

            for (int k = 0; k < centroidsPerSubSpaceCount; k++) {
                distanceTable[m][k] = Vectors.squaredEuclidianDistance(querySubvector, codebooks[m][k]);
            }
        }

        return distanceTable;
    }

    /**
     * Compute asymmetric distance from query to encoded vector using pre-computed distance table
     */
    private double asymmetricDistance(double[][] distanceTable, byte[] codes) {
        double totalDist = 0;

        for (int m = 0; m < subSpacesCount; m++) {
            int code = codes[m] & 0xFF; // Convert to unsigned
            totalDist += distanceTable[m][code];
        }

        return Math.sqrt(totalDist);
    }

    /**
     * Extract the m-th subvector from a vector
     */
    private double[] extractSubvector(double[] vector, int m) {
        double[] subvector = new double[subvectorDim];
        int startIdx = m * subvectorDim;
        System.arraycopy(vector, startIdx, subvector, 0, subvectorDim);
        return subvector;
    }

    /**
     * Encode a vector into PQ codes
     */
    public byte[] encode(double[] vector) {
        if (codebooks == null) {
            throw new IllegalStateException("Codebooks not trained. Call buildSubspaces() first.");
        }

        byte[] codes = new byte[subSpacesCount];

        for (int m = 0; m < subSpacesCount; m++) {
            double[] subvector = extractSubvector(vector, m);
            int nearestCentroid = findNearestCentroid(subvector, codebooks[m]);
            codes[m] = (byte) nearestCentroid;
        }

        return codes;
    }

    /**
     * Find the nearest centroid to a subvector
     */
    private int findNearestCentroid(double[] subvector, double[][] centroids) {
        int nearest = 0;
        double minDist = Double.MAX_VALUE;

        for (int k = 0; k < centroids.length; k++) {
            double dist = Vectors.squaredEuclidianDistance(subvector, centroids[k]);
            if (dist < minDist) {
                minDist = dist;
                nearest = k;
            }
        }

        return nearest;
    }

    private int calculateOptimalSubSpacesCount(int dimension) {
        if (dimension <= 128) return 4;
        if (dimension <= 256) return 8;
        if (dimension <= 512) return 16;
        if (dimension <= 1024) return 32;
        return 64;
    }
}
