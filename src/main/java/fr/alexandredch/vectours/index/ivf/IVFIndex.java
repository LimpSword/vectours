package fr.alexandredch.vectours.index.ivf;

import fr.alexandredch.vectours.data.Vector;
import java.util.List;

public interface IVFIndex {

    boolean canSearch();

    void insertVector(Vector vector);

    List<Vector> search(double[] vector, int nprobe);
}
