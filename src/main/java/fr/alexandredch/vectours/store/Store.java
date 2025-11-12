package fr.alexandredch.vectours.store;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Store {

    void initFromDisk();

    CompletableFuture<Void> insert(Vector vector);

    List<SearchResult> search(double[] vector, int k);

    List<SearchResult> search(SearchParameters searchParameters);

    CompletableFuture<Void> delete(String id);

    Vector getVector(String id);

    void dropAll();

    void saveAll();
}
