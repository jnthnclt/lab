package com.github.jnthnclt.os.lab.nn;

public class NN {

    /*
    Euclidian Distance without the final sqrt
     */
    static public double comparableEuclidianDistance(double[] a, double[] b) {
        double v = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            v += (d * d);
        }
        return v;
    }
}
