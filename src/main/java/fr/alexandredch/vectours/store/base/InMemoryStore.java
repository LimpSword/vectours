package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.SearchResult;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.math.Vectors;
import fr.alexandredch.vectours.serialization.InMemorySerializer;
import fr.alexandredch.vectours.store.Store;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryStore implements Store {

    public static final String STORE_FILE_NAME = "vectours_store.dat";

    private final Map<String, Vector> store = new ConcurrentHashMap<>();
    private final WriteAheadLogger writeAheadLogger = new WriteAheadLogger();

    @Override
    public void initFromDisk() {
        File file = new File(STORE_FILE_NAME);
        if (file.exists()) {
            try {
                byte[] bytes = Files.readAllBytes(file.toPath());
                store.putAll(InMemorySerializer.deserialize(bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

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
                        e.getValue().getId(), e.getKey(), e.getValue().getMetadata()))
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

    @Override
    public void save() {
        try {
            byte[] data = InMemorySerializer.serialize(store);
            Files.write(new File(STORE_FILE_NAME).toPath(), data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
