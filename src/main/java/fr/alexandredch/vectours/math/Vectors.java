package fr.alexandredch.vectours.math;

public class Vectors {

    public static double euclideanDistance(double[] values1, double[] values2) {
        if (values1.length != values2.length) {
            throw new IllegalArgumentException("Vectors must have the same length");
        }

        double sum = 0.0;
        for (int i = 0; i < values1.length; i++) {
            sum += Math.pow(values1[i] - values2[i], 2);
        }

        return Math.sqrt(sum);
    }
}
