package fr.alexandredch.vectours.store;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import java.util.List;

public interface Store {

    void initFromDisk();

    void insert(Vector vector);

    List<SearchResult> search(double[] vector, int k);

    List<SearchResult> search(SearchParameters searchParameters);

    void delete(String id);

    Vector getVector(String id);

    void dropAll();

    void saveAll();
}
