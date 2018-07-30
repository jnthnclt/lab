package com.github.jnthnclt.os.lab.nn;

import java.util.Arrays;
import java.util.Comparator;
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
            return absInsertion(Arrays.binarySearch(neighbors, n, nComparator));
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

    public static N naive2(N query, N[] neighbors) {

        int features = neighbors[0].features.length;
        double[] lows = new double[features];
        double[] highs = new double[features];
        Arrays.fill(lows, -Double.MAX_VALUE);
        Arrays.fill(highs, Double.MAX_VALUE);

        double min = Double.MAX_VALUE;
        N nearest = null;
        int count = 0;
        int skipped = 0;
        NEXT:
        for (N neighbor : neighbors) {
//            for (int i = 0; i < features; i++) {
//                double f = neighbor.features[i];
//                if (f < lows[i]) {
//                    skipped++;
//                    continue NEXT;
//                }
//                if (f > highs[i]) {
//                    skipped++;
//                    continue NEXT;
//                }
//            }
            count++;
            double d = euclidianDistance(query.features, neighbor.features, -1);
            if (d < min) {
                min = d;
                nearest = neighbor;

                for (int i = 0; i < features; i++) {
                    double partialDistance = euclidianDistance(query.features, nearest.features, i);
                    lows[i] = low(min, nearest.features[i], partialDistance);
                    highs[i] = high(min, nearest.features[i], partialDistance);
                }

                hl("", lows, highs);
            }
        }

        System.out.println("count:" + count + " skipped:" + skipped);
        return nearest;
    }

    public static int absInsertion(int binarysearchIndex) {
        return binarysearchIndex < 0 ? (-(binarysearchIndex) - 1) : binarysearchIndex;
    }

    public static void main(String[] args) {


        Random rand = new Random();

        int numFeatures = 4;
        int numN = 1000;
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
            //n = fancy(query, dimensions);
            n = naive2(query, neighbors);
            ids += n.id;
        }
        long elapse = System.currentTimeMillis() - timestamp;

        System.out.println(elapse + " fancy answer:" + n + " " + ids);


        timestamp = System.currentTimeMillis();
        ids = 0;
        for (int i = 0; i < loops; i++) {
            n = naive(query, neighbors);
            ids += n.id;
        }
        elapse = System.currentTimeMillis() - timestamp;
        System.out.println(elapse + " naive answer:" + n + " " + ids);
        System.out.println("query:" + query);

    }


    public static N fancy(N query, Dimension[] dimensions) {

        double min = Double.MAX_VALUE;
        N nearest = query;
        int count = 0;
        int totalCount = 0;

        double[] lows = new double[dimensions.length];
        double[] highs = new double[dimensions.length];
        Arrays.fill(lows, -Double.MAX_VALUE);
        Arrays.fill(highs, Double.MAX_VALUE);

        //N search = qnew N(-1, Arrays.copyOf(query.features, query.features.length));

        int[] dc = new int[dimensions.length];
        Arrays.fill(dc, Integer.MAX_VALUE);
        int[] l = new int[dimensions.length];
        Arrays.fill(l, 1);
        int[] h = new int[dimensions.length];
        Arrays.fill(h, 1);
        N[] ns = new N[dimensions.length];

        boolean more = true;
        while (more) {
            int start = totalCount;
            for (int d = 0; d < dimensions.length; d++) {
                ns[d] = null;
                if (dc[d] == Integer.MAX_VALUE) {
                    dc[d] = dimensions[d].closest(nearest);
                    ns[d] = dimensions[d].nn(dc[d]);
                } else {
                    while (true) {
                        if (l[d] > 0 && h[d] > 0) {
                            if (l[d] > h[d]) {
                                ns[d] = dimensions[d].n(dc[d] + h[d]);
                                h[d]++;
                                if (ns[d] == null || ns[d].features[d] > highs[d]) {
                                    h[d] = -h[d];
                                    ns[d] = null;
                                }
                            } else {
                                ns[d] = dimensions[d].n(dc[d] - l[d]);
                                l[d]++;
                                if (ns[d] == null || ns[d].features[d] < lows[d]) {
                                    l[d] = -l[d];
                                    ns[d] = null;
                                }
                            }
                        } else if (l[d] > 0) {
                            ns[d] = dimensions[d].n(dc[d] - l[d]);
                            l[d]++;
                            if (ns[d] == null || ns[d].features[d] < lows[d]) {
                                l[d] = -l[d];
                                ns[d] = null;
                            }
                        } else if (h[d] > 0) {
                            ns[d] = dimensions[d].n(dc[d] + h[d]);
                            h[d]++;
                            if (ns[d] == null || ns[d].features[d] > highs[d]) {
                                h[d] = -h[d];
                                ns[d] = null;
                            }
                        }
                        if (ns[d] != null && ns[d].used == false) {
                            break;
                        }
                        if (h[d] < 0 && l[d] < 0) {
                            break;
                        }
                    }
                }

                if (ns[d] != null) {
                    totalCount++;

                    double ed = euclidianDistance(query.features, ns[d].features, -1);
                    if (ed < min) {
                        min = ed;
                        nearest = ns[d];

                        for (int i = 0; i < dimensions.length; i++) {
                            double partialDistance = euclidianDistance(query.features, ns[d].features, i);
                            double nl = low(min, query.features[i], partialDistance);
                            double nh = high(min, query.features[i], partialDistance);
                            lows[i] = nl;
                            highs[i] = nh;
                        }

                        //hl("", lows, highs);

                    }
                }
            }
            if (start == totalCount) {
                break;
            }
        }
        System.out.println(totalCount + " vs " + dimensions[0].neighbors.length);

//        while (d < dimensions.length) {
//            N n = null;
//            if (dc == Integer.MAX_VALUE) {
//                dc = dimensions[d].closest(nearest);
//                n = dimensions[d].nn(dc);
//            } else {
//                while (true) {
//                    n = null;
//                    if (l > 0 && h > 0) {
//                        if (l > h) {
//                            n = dimensions[d].n(dc + h);
//                            h++;
//                            if (n == null || n.features[d] > highs[d]) {
//                                h = -h;
//                                n = null;
//                            }
//                        } else {
//                            n = dimensions[d].n(dc - l);
//                            l++;
//                            if (n == null || n.features[d] < lows[d]) {
//                                l = -l;
//                                n = null;
//                            }
//                        }
//                    } else if (l > 0) {
//                        n = dimensions[d].n(dc - l);
//                        l++;
//                        if (n == null || n.features[d] < lows[d]) {
//                            l = -l;
//                            n = null;
//                        }
//                    } else if (h > 0) {
//                        n = dimensions[d].n(dc + h);
//                        h++;
//                        if (n == null || n.features[d] > highs[d]) {
//                            h = -h;
//                            n = null;
//                        }
//                    }
//
//                    if (n != null && n.used == false) {
//                        break;
//                    }
//                    if (h < 0 && l < 0) {
//                        break;
//                    }
//                }
//            }
//            if (n == null) {
//                System.out.println("dim=" + d
//                    + " min=" + dimensions[d].min
//                    + " max=" +  dimensions[d].max
//                    + " count=" + count
//                    + " c=" + dc
//                    + " l=" + Math.abs(l)
//                    + " h=" + Math.abs(h)
//                    + " lows=" + lows[d]
//                    + " highs=" + highs[d]);
//                d++;
//                l = 1;
//                h = 1;
//                dc = Integer.MAX_VALUE;
//                count = 0;
//
//                if (d < dimensions.length) {
//                    double partialDistance = euclidianDistance(query.features, nearest.features, d);
//                    lows[d] = low(min, query.features[d], partialDistance);
//                    highs[d] = high(min, query.features[d], partialDistance);
//                    //search.features[d] = lows[d] + ((highs[d] - lows[d]) / 2);
//                }
//                continue;
//            }
//            count++;
//            totalCount++;
//
//            n.used = true;
//
//            double partialDistance = euclidianDistance(query.features, n.features, d);
//            double v = query.features[d] - n.features[d];
//            double ed = partialDistance + (v * v);
//            if (ed < min) {
//                min = ed;
//                nearest = n;
//                lows[d] = low(min, query.features[d], partialDistance);
//                highs[d] = high(min, query.features[d], partialDistance);
//            }
//        }

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
        return -(Math.sqrt(best - partialDistance) - dim);
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
        return -(-Math.sqrt(best + partialDistance) - dim);
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
