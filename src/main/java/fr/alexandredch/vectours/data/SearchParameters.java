package fr.alexandredch.vectours.data;

public record SearchParameters(double[] searchedVector, boolean allowIVF, int topK) {}
