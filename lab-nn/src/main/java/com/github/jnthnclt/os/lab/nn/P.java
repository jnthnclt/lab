package com.github.jnthnclt.os.lab.nn;

import java.util.Arrays;
import java.util.Random;

public class P {


    public static P random(Random random, int id, int numFeatures) {
        double[] features = new double[numFeatures];
        for (int i = 0; i < features.length; i++) {
            features[i] = random.nextDouble();
        }
        return new P(id, features);
    }


    public final int id;
    public final double[] features;

    public P(int id, double[] features) {
        this.id = id;
        this.features = features;
    }

    @Override
    public String toString() {
        return "N{" +
            "id=" + id +
            ", features=" + Arrays.toString(features) +
            '}';
    }
}
