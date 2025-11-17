package fr.alexandredch.vectours.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public record Vector(String id, double[] values, @Nullable Metadata metadata) implements Serializable {

    @Override
    public String toString() {
        return id() + ":" + Arrays.toString(values());
    }

    public Vector withId(String newId) {
        return new Vector(newId, this.values, this.metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Vector vector = (Vector) o;
        return Objects.equals(id, vector.id)
                && Objects.deepEquals(values, vector.values)
                && Objects.equals(metadata, vector.metadata);
    }
}
