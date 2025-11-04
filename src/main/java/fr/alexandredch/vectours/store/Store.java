package fr.alexandredch.vectours.store;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;

import java.util.List;

public interface Store {

    void initFromDisk();

    void insert(String id, Vector vector);

    List<SearchResult> search(float[] vector, int k);

    void delete(String id);

    Vector getVector(String id);

    void dropAll();

    void save();
}
