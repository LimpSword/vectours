package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Vector;

import java.util.*;

public final class Segment {

    private static final int SEGMENT_SIZE = 1000;

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
        dirty = true;
        vectors.put(vector.id(), vector);
        ids.add(vector.id());
    }

    public void delete(String id) {
        tombstones.add(id);
        ids.remove(id);
    }

    public boolean containsId(String id) {
        return ids.contains(id);
    }

    public Vector getVector(String id) {
        for (Vector vector : vectors.values()) {
            if (vector.id().equals(id)) {
                return vector;
            }
        }
        return null;
    }

    public Collection<Vector> getVectors() {
        return List.copyOf(vectors.values());
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
        return vectors.size() >= SEGMENT_SIZE;
    }
}
