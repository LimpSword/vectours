package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Vector;
import java.util.*;

public final class Segment {

    public static final int MAX_SEGMENT_SIZE = 1000;

    private final Map<String, Vector> vectors = new HashMap<>();
    // TODO: Use a more memory-efficient structure for IDs and bloom filters?
    // Duplicate storage of IDs for faster lookup and because we don't rewrite vectors map on deletions (yet)
    private final Set<String> ids = new HashSet<>();
    private final Set<String> tombstones = new HashSet<>();
    private final int id;
    private boolean dirty = false;

    public Segment(int segmentId) {
        this.id = segmentId;
    }

    public void insert(Vector vector) {
        if (isFull()) {
            throw new IllegalStateException("Segment is full");
        }
        tombstones.remove(vector.id());

        dirty = true;
        vectors.put(vector.id(), vector);
        ids.add(vector.id());
    }

    public void delete(String id) {
        if (!ids.contains(id) || tombstones.contains(id)) {
            return;
        }
        dirty = true;
        tombstones.add(id);
        ids.remove(id);
    }

    public int size() {
        return vectors.size() - tombstones.size();
    }

    public boolean containsId(String id) {
        return ids.contains(id);
    }

    public Vector getVector(String id) {
        return vectors.get(id);
    }

    public Collection<Vector> getVectors() {
        List<Vector> vectorList = new ArrayList<>();
        for (Vector vector : vectors.values()) {
            if (!tombstones.contains(vector.id())) {
                vectorList.add(vector);
            }
        }
        return List.copyOf(vectorList);
    }

    public Collection<String> getTombstones() {
        return List.copyOf(tombstones);
    }

    public int getId() {
        return id;
    }

    public Segment setDirty(boolean dirty) {
        this.dirty = dirty;
        return this;
    }

    public boolean isDirty() {
        return dirty;
    }

    public boolean isFull() {
        return vectors.size() >= MAX_SEGMENT_SIZE;
    }
}
