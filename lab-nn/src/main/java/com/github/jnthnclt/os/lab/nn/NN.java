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



    static public double[] avg(double[] as, double[] bs) {
        double[] ns = new double[as.length];
        for (int i = 0; i < as.length; i++) {
            ns[i] = (as[i] + bs[i]) / 2.0;
        }
        return ns;
    }

    static public double[] max(double[] as, double[] bs) {
        double[] ns = new double[as.length];
        for (int i = 0; i < as.length; i++) {
            ns[i] = Math.max(as[i], bs[i]);
        }
        return ns;
    }

    static public double[] min(double[] as, double[] bs) {
        double[] ns = new double[as.length];
        for (int i = 0; i < as.length; i++) {
            ns[i] = Math.min(as[i], bs[i]);
        }
        return ns;
    }

    public static void sum(double[] features, double[] add) {
        for (int i = 0; i < features.length; i++) {
            features[i] += add[i];
        }
    }
}
