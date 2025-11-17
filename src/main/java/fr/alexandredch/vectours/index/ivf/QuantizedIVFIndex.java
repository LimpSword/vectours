package fr.alexandredch.vectours.index.ivf;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.pq.VectorProductQuantization;
import java.util.List;

public final class QuantizedIVFIndex implements IVFIndex {

    public static final int MIN_VECTORS_FOR_IVF_INDEX = 10_000;

    private final VectorProductQuantization productQuantization = new VectorProductQuantization(this);

    @Override
    public boolean canSearch() {
        return false;
    }

    @Override
    public void insertVector(Vector vector) {}

    @Override
    public List<Vector> search(double[] vector, int nprobe) {
        return List.of();
    }

    public List<double[]> getCentroids(int index) {
        return List.of();
    }
}
