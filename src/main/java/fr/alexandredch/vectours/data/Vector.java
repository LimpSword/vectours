package fr.alexandredch.vectours.data;

import java.io.Serializable;
import java.util.Arrays;

public record Vector(String id, double[] values, Metadata metadata) implements Serializable {

    @Override
    public String toString() {
        return id() + ":" + Arrays.toString(values());
    }
}
