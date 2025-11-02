package fr.alexandredch.vectours.store;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Vectors;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryStore implements Store {

    private final Map<String, Vector> store = new ConcurrentHashMap<>();

    @Override
    public void insert(String id, Vector vector) {
        store.put(id, vector);
    }

    @Override
    public List<SearchResult> search(float[] vector, int k) {
        TreeMap<Double, Vector> map = new TreeMap<>();
        for (Map.Entry<String, Vector> entry : store.entrySet()) {
            double distance = Vectors.euclideanDistance(vector, entry.getValue().getValues());
            map.put(distance, entry.getValue());
        }

        return map.entrySet().stream()
                .limit(k)
                .map(e -> new SearchResult(
                        e.getValue().getId(),
                        e.getKey(),
                        e.getValue().getMetadata()
                ))
                .toList();
    }

    @Override
    public void delete(String id) {
        store.remove(id);
    }

    @Override
    public Vector getVector(String id) {
        return store.get(id);
    }

    @Override
    public void dropAll() {
        store.clear();
    }
}
