package fr.alexandredch.vectours.data;

import java.io.Serializable;

public record Vector(String id, float[] values, Metadata metadata) implements Serializable {}
