package fr.alexandredch.vectours.data;

public final class Vector {

    private final String id;
    private final float[] values;
    private final Metadata metadata;

    public Vector(String id, float[] values, Metadata metadata) {
        this.id = id;
        this.values = values;
        this.metadata = metadata;
    }

    public String getId() {
        return id;
    }

    public float[] getValues() {
        return values;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
