package fr.alexandredch.vectours.math;

import fr.alexandredch.vectours.data.Vector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Cluster {

    private final double[] centroid;
    private List<Vector> data;

    public Cluster(double[] centroid) {
        this.centroid = centroid;
        this.data = new ArrayList<>();
    }

    public double[] getCentroid() {
        return centroid;
    }

    public List<Vector> getData() {
        return data;
    }

    public void setData(List<Vector> data) {
        this.data = data;
    }

    public void addVector(Vector vector) {
        this.data.add(vector);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Cluster cluster = (Cluster) obj;
        return Arrays.equals(centroid, cluster.centroid);
    }
}
