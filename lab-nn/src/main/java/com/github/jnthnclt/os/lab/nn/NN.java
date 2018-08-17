package com.github.jnthnclt.os.lab.nn;

public class NNUtil {

    static public double euclidianDistance(double[] a, double[] b, int exclude) {
        double v = 0;
        for (int i = 0; i < a.length; i++) {
            if (i != exclude) {
                double d = a[i] - b[i];
                v += (d * d);
            }
        }
        return v;
    }
}
