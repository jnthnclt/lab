package com.github.jnthnclt.os.lab.nn;

import java.util.Random;

public class P {


    public static P random(Random random,  int numFeatures, double[][] bounds) {
        double[] features = new double[numFeatures];
        for (int i = 0; i < features.length; i++) {
            double r = random.nextDouble();
            if (bounds != null && i < bounds.length) {
                features[i] = bounds[i][0] + ((bounds[i][1] - bounds[i][0]) * r);
            } else {
                features[i] = r;
            }
        }
        return new P(features);
    }


    public double[] features;

    public P(double[] features) {
        this.features = features;
    }


}
