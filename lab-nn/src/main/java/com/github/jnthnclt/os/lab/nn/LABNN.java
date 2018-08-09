package com.github.jnthnclt.os.lab.nn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class LABNN {

    static class N {
        private final long id;
        private final double[] features;
        public boolean used = false;

        N(long id, double[] features) {
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

    public static N randomN(Random random, long id, int numFeatures) {
        double[] features = new double[numFeatures];
        for (int i = 0; i < features.length; i++) {
            features[i] = random.nextDouble();
        }
        return new N(id, features);
    }

    static class Dimension {
        private final N[] neighbors;
        private final Comparator<N> nComparator;
        private final double min;
        private final double max;

        Dimension(int featureIndex, N[] neighbors) {
            this.nComparator = Comparator.comparingDouble(o -> o.features[featureIndex]);

            N[] sort = Arrays.copyOf(neighbors, neighbors.length);
            Arrays.sort(sort, nComparator);
            this.neighbors = sort;
            this.min = sort[0].features[featureIndex];
            this.max = sort[sort.length - 1].features[featureIndex];
        }

        int closest(N n) {
            return Math.min(absInsertion(Arrays.binarySearch(neighbors, n, nComparator)), neighbors.length - 1);
        }

        N n(int i) {
            return i < 0 ? null : i >= neighbors.length ? null : neighbors[i];
        }

        N nn(int i) {
            return i < 0 ? neighbors[0] : i >= neighbors.length ? neighbors[neighbors.length - 1] : neighbors[i];
        }
    }


    public static N naive(N query, N[] neighbors) {

        double min = Double.MAX_VALUE;
        N nearest = null;
        for (N neighbor : neighbors) {
            double d = euclidianDistance(query.features, neighbor.features, -1);
            if (d < min) {
                min = d;
                nearest = neighbor;
            }
        }
        return nearest;
    }


    public static int absInsertion(int binarysearchIndex) {
        return binarysearchIndex < 0 ? (-(binarysearchIndex) - 1) : binarysearchIndex;
    }

    public static void main(String[] args) {

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        int numFeatures = 100;
        int numN = 100;
        int loops = 1;

        N[] neighbors = new N[numN];
        for (int i = 0; i < neighbors.length; i++) {
            neighbors[i] = randomN(rand, i, numFeatures);
        }


        N query = randomN(rand, -1, numFeatures);


        Dimension[] dimensions = new Dimension[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            dimensions[i] = new Dimension(i, neighbors);
        }
        N n = null;
        long timestamp = System.currentTimeMillis();
        long ids = 0;
        for (int i = 0; i < loops; i++) {
            n = fancy(query, dimensions);
            //n = naive2(query, neighbors);
            ids += n.id;
        }
        long elapse = System.currentTimeMillis() - timestamp;

        System.out.println(euclidianDistance(n.features, query.features, -1) + "|" + elapse + " fancy answer:" + n + " " + ids);


        timestamp = System.currentTimeMillis();
        ids = 0;
        for (int i = 0; i < loops; i++) {
            n = naive(query, neighbors);
            ids += n.id;
        }
        elapse = System.currentTimeMillis() - timestamp;
        System.out.println(euclidianDistance(n.features, query.features, -1) + "|" + elapse + " naive answer:" + n + " " + ids);
        System.out.println("query:" + query);
        System.out.println("seed:" + seed);

    }

    public static N fancy(N query, Dimension[] dimensions) {

        double min = Double.MAX_VALUE;
        N nearest = null;

        int gets = 0;

        int dl = dimensions.length;
        int[] fis = new int[dl];
        int[] highs = new int[dl];
        int[] lows = new int[dl];

        int max_neighbors = dimensions[0].neighbors.length;

        Map<Long, Integer> counts = Maps.newHashMap();
        for (int i = 0; i < dl; i++) {
            gets++;
            fis[i] = lows[i] = highs[i] = dimensions[i].closest(query);
            long id = dimensions[i].nn(fis[i]).id;
            counts.put(id, 1);
            System.out.println("S " + i + " i=" + fis[i] + " id=" + id + " count=1");
        }
        int topN = 1;
        List<N> nearests = Lists.newArrayList();
        while (nearests.isEmpty() && topN >= 0) {

            for (int i = 0; i < dl; i++) {
                if (lows[i] > 0) {
                    lows[i]--;
                    gets++;
                    long id = dimensions[i].nn(lows[i]).id;
                    Integer c = counts.compute(id, (k, v) -> v == null ? 1 : v + 1);
                    if (c == dl) {
                        nearests.add(dimensions[i].nn(lows[i]));
                    }

                    System.out.println("L " + i + " i=" + lows[i] + " id=" + id + " count=" + c);
                }

                if (highs[i] < max_neighbors - 1) {
                    highs[i]++;
                    gets++;
                    long id = dimensions[i].nn(highs[i]).id;
                    Integer c = counts.compute(id, (k, v) -> v == null ? 1 : v + 1);
                    if (c == dl) {
                        nearests.add(dimensions[i].nn(highs[i]));
                    }
                    System.out.println("H " + i + " i=" + highs[i] + " id=" + id + " count=" + c);
                }
            }

            if (!nearests.isEmpty()) {
                int better = 0;
                for (N n : nearests) {
                    double d = euclidianDistance(n.features, query.features, -1);
                    if (d < min) {
                        min = d;
                        nearest = n;
                        better++;
                    }
                }
                if (better == 0) {
                    topN--;
                }
                nearests.clear();
            }

        }


        System.out.println("gets:" + gets);
        return nearest;

    }


    public static N fancy_hmm(N query, Dimension[] dimensions) {

        double min = Double.MAX_VALUE;
        N nearest = query;
        int compares = 0;

        int dl = dimensions.length;
        int[] fis = new int[dl];
        int[] highs = new int[dl];
        int[] cis = new int[dl];
        int[] lows = new int[dl];


        int max_neighbors = dimensions[0].neighbors.length;
        int di = 0;
        while (di < dl) {

            fis[di] = lows[di] = highs[di] = cis[di] = dimensions[di].closest(nearest);

            N n = dimensions[di].nn(cis[di]);

            double pd = euclidianDistance(query.features, n.features, di);
            double delta = query.features[di] = n.features[di];
            double d = pd + (delta * delta);
            compares++;
            if (d < min) {
                nearest = n;
                min = d;
            }

            double low = low(min, nearest.features[di], pd * dl);
            double high = high(min, nearest.features[di], pd * dl);
            System.out.println(di + " " + low + " " + nearest.features[di] + " " + high);
            boolean lowDone = false;
            boolean highDone = false;
            while ((!lowDone && lows[di] > 0) || (!highDone && highs[di] < max_neighbors - 1)) {
                if (!lowDone && lows[di] > 0) {
                    lows[di]--;
                    n = dimensions[di].nn(lows[di]);
                    if (n.features[di] >= low) {
                        pd = euclidianDistance(query.features, n.features, di);
                        compares++;
                        delta = query.features[di] = n.features[di];
                        d = pd + (delta * delta);
                        if (d < min) {
                            lowDone = false;
                            highDone = false;
                            nearest = n;
                            min = d;
                            low = low(min, nearest.features[di], pd * dl);
                            high = high(min, nearest.features[di], pd * dl);
                            System.out.println(di + " LOW CLOSER:" + d + " " + low + " " + nearest.features[di] + " " + high);
                        }
                    } else {
                        lowDone = true;
                        System.out.println(di + " LOW DONE:" + n.features[di] + " >= " + low);
                    }
                }
                if (!highDone && highs[di] < max_neighbors - 1) {
                    highs[di]++;
                    n = dimensions[di].nn(highs[di]);
                    if (n.features[di] <= high) {
                        pd = euclidianDistance(query.features, n.features, di);
                        compares++;
                        delta = query.features[di] = n.features[di];
                        d = pd + (delta * delta);
                        if (d < min) {
                            lowDone = false;
                            highDone = false;
                            nearest = n;
                            min = d;
                            low = low(min, nearest.features[di], pd * dl);
                            high = high(min, nearest.features[di], pd * dl);
                            System.out.println(di + " HIGH CLOSER:" + d + " " + low + " " + nearest.features[di] + " " + high);
                        }
                    } else {
                        highDone = true;
                        System.out.println(di + " HIGH DONE:" + n.features[di] + " <= " + high);
                    }
                }
            }
            di++;
        }

        System.out.println(compares + " " + max_neighbors);
        return nearest;
    }

    static private void hl(String context, double[] lows, double[] highs) {
        for (int i = 0; i < lows.length; i++) {
            System.out.print((i > 0 ? "\t" : "") + lows[i] + "\t" + highs[i]);
        }
        System.out.println();

        //System.out.println(context + " lows:" + Arrays.toString(lows) + " highs:" + Arrays.toString(highs));
    }

    // Math.sqrt( ((x1-x2)^2) + ((y1-y2)^2)
    // ((x1-x2)^2) + ((y1-y2)^2) // lose unnecessary sqrt
    // ((10-x)^2) + ((10-15)^2) < ((10-20)^2) + ((30-15)^2)
    // (10 - x)^2 + 25 < 325
    // (10 - x)^2 < 325 - 25
    // 10 - x < sqrt(325 - 25)
    // -x < sqrt(325 - 25) - 10
    // x < -(sqrt(325 - 25) - 10)
    public static double low(double best, double dim, double partialDistance) {
        double v = -(Math.sqrt(best - partialDistance) - dim);
        return Double.isNaN(v) ? 0.0 : v;
    }


    // (10 - x)^2 + 25 < 325
    // 25 < 325 - (10 - x)^2
    // 325 + 25 < -((10 - x)^2)
    // 325 + 25 < -((10 - x)^2)
    // sqrt(325 + 25) < -(10 - x)
    // -sqrt(325 + 25) < 10 - x
    // -sqrt(325 + 25) - 10 < -x
    // -(-sqrt(325 + 25) - 10) < x
    public static double high(double best, double dim, double partialDistance) {
        double v = -(-Math.sqrt(best + partialDistance) - dim);
        return Double.isNaN(v) ? 0.0 : v;
    }


    //
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
