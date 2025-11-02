package fr.alexandredch.vectours;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;

import java.util.List;

public interface Vectours {

    void insert(String id, Vector vector);

    List<SearchResult> search(float[] vector, int k);

    void delete(String id);

    Vector getVector(String id);
}
