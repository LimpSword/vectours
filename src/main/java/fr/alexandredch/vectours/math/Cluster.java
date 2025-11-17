package fr.alexandredch.vectours.math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Cluster<T> {

    private final double[] centroid;
    private List<T> data;

    public Cluster(double[] centroid) {
        this.centroid = centroid;
        this.data = new ArrayList<>();
    }

    public double[] getCentroid() {
        return centroid;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public void add(T vector) {
        this.data.add(vector);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Cluster<T> cluster = (Cluster<T>) obj;
        return Arrays.equals(centroid, cluster.centroid);
    }
}
