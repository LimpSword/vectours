package fr.alexandredch.vectours.data;

public final class Vector {

    private final float[] values;
    private final Metadata metadata;

    public Vector(float[] values, Metadata metadata) {
        this.values = values;
        this.metadata = metadata;
    }

    public float[] getValues() {
        return values;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
