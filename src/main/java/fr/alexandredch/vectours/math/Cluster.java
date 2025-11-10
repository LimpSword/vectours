package fr.alexandredch.vectours.math;

import java.util.Arrays;

public final class Cluster {

    private final double[] centroid;
    private double[][] data;

    public Cluster(double[] centroid) {
        this.centroid = centroid;
        this.data = new double[0][];
    }

    public double[] getCentroid() {
        return centroid;
    }

    public double[][] getData() {
        return data;
    }

    public void setData(double[][] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Cluster cluster = (Cluster) obj;
        return Arrays.equals(centroid, cluster.centroid);
    }
}
