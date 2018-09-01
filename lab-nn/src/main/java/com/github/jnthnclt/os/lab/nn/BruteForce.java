package com.github.jnthnclt.os.lab.nn;

public class BruteForce {

    public static P bruteForce(P query, P[] neighbors, Distance distance) {

        double min = Double.MAX_VALUE;
        P nearest = null;
        for (P neighbor : neighbors) {
            double d = distance.distance(query.features, neighbor.features);
            if (d < min) {
                min = d;
                nearest = neighbor;
            }
        }
        return nearest;
    }
}
