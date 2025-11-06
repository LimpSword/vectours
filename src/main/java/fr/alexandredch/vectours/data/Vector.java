package fr.alexandredch.vectours.data;

import java.io.Serializable;

public record Vector(String id, double[] values, Metadata metadata) implements Serializable {}
