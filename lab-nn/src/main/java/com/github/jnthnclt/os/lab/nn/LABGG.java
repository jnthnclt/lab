package com.github.jnthnclt.os.lab.nn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class LABNG {

    static class N {
        private final long id;
        private final double[] features;

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


    static String[] color = new String[] { "green", "orange", "red", "purple", "blue", "cyan", "gray", "black" };

    static class NG {
        private final N n;
        NG[] nearest;
        int depth = 0;

        NG(N n) {
            this.n = n;
        }

        public void asString() {
            for (int i = 0; i < nearest.length; i++) {
                System.out.println(n.id + " -> " + nearest[i].n.id + "[ color=\"" + color[Math.min(i, color.length - 1)] + "\"];");
            }
        }

        public NG find(N find, AtomicLong count, Set<Integer> starts) {
            count.incrementAndGet();
            double d = euclidianDistance(n.features, find.features, -1);
            int displace = -1;
            for (int i = 0; i < nearest.length; i++) {

//                if (starts.contains((int)nearest[i].n.id)) {
//                    continue;
//                }

                count.incrementAndGet();
                double ed = euclidianDistance(nearest[i].n.features, find.features, -1);
                if (ed < d) {
                    d = ed;
                    displace = i;
                }
            }
            return displace == -1 ? this : nearest[displace];
        }


    }

    public static void main(String[] args) {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        int topN = 1;
        int numFeatures = 2;
        int numN = 1024;
        int maxZ = 0;

        N[] neighbors = new N[numN];
        for (int i = 0; i < neighbors.length; i++) {
            neighbors[i] = randomN(rand, i, numFeatures);
        }

        NG[] ngs = new NG[neighbors.length];
        Map<Integer, NG> lut = Maps.newHashMap();
        for (int i = 0; i < neighbors.length; i++) {
            int si = i;
            NG startNG = lut.computeIfAbsent(si, integer -> new NG(neighbors[si]));
            ngs[i] = startNG;

            int[] is = topN(i, neighbors, topN);
            NG[] topNGs = new NG[topN];
            for (int j = 0; j < topNGs.length; j++) {
                int ni = is[j];
                topNGs[j] = lut.computeIfAbsent(ni, integer -> new NG(neighbors[ni]));
            }
            startNG.nearest = topNGs;
        }

//        Foo:
//        {
//            for (int z = 0; z < maxZ; z++) {
//
//
//
//
//                Map<Long, Integer> tos = Maps.newHashMap();
//                for (NG n : ngs) {
//                    tos.compute(n.n.id, (k, v) -> v == null ? 1 : v + 1);
//                    for (int i = 0; i < n.nearest.length; i++) {
//                        tos.compute(n.nearest[i].n.id, (k, v) -> v == null ? 1 : v + 1);
//                    }
//                }
//
//                Set<Integer> starts = Sets.newHashSet();
//                for (Entry<Long, Integer> entry : tos.entrySet()) {
//                    if (entry.getValue() == 1) {
//                        starts.add((int) (long) entry.getKey());
//                    }
//                }
//
//                if (starts.size() <= 2) {
//                    break;
//                }
//
//                N[] mutualNs = new N[starts.size()];
//                int p = 0;
//                for (Integer start : starts) {
//                    mutualNs[p] = neighbors[start];
//                    p++;
//                }
//
//                for (int i = 0; i < mutualNs.length; i++) {
//                    NG startNG = lut.get((int) mutualNs[i].id);
//                    startNG.depth++;
//
//                    int[] is = topN(i, mutualNs, 1);
//                    NG[] topNGs = new NG[1];
//                    for (int j = 0; j < topNGs.length; j++) {
//                        topNGs[j] = lut.get(is[j]);
//                    }
//
//                    NG next = startNG;
//                    NG[] grow = Arrays.copyOf(next.nearest, next.nearest.length + 1);
//                    grow[grow.length - 1] = topNGs[0];
//                    next.nearest = grow;
//
//                }
//            }
//        }


        // Find mutual
        Set<Integer> ends = null;
        Set<Integer> mutual = Sets.newHashSet();
        int depth = 0;
        do {

            mutual.clear();
            depth++;


            for (int i = 0; i < ngs.length; i++) {
                NG at = ngs[i];

                for (int j = 0; j < at.nearest.length; j++) {
                    NG next = at.nearest[j];
                    for (int k = 0; k < next.nearest.length; k++) {
                        if (next.nearest[k] == at) {
                            System.out.println(at.n.id + "<->" + next.n.id);

                            mutual.add((int) at.n.id);
                            mutual.add((int) next.n.id);
                        }
                    }
                }
            }

            ends = mutual;
            break;
//            if (ends.size() <= depth + 1) {
//                break;
//            }
//
//            N[] mutualNs = new N[mutual.size()];
//            int m = 0;
//            for (Integer ni : mutual) {
//                mutualNs[m] = neighbors[ni];
//                m++;
//            }
//
//
//            for (int i = 0; i < mutualNs.length; i++) {
//                NG startNG = lut.get((int) mutualNs[i].id);
//                startNG.depth++;
//
//                int[] is = topN(i, mutualNs, depth + 1);
//                NG[] topNGs = new NG[1];
//                for (int j = 0; j < topNGs.length; j++) {
//                    topNGs[j] = lut.get(is[depth + j]);
//                }
//                startNG.nearest = topNGs;
//            }


        }
        while (mutual.size() > 0);


        System.out.println(ends);
        if (2 + 2 == 4) {
            return;
        }


        System.out.println();


        for (NG n : ngs) {
            n.asString();

        }
        System.out.println();


        NG[] reversed = new NG[neighbors.length];
        for (int i = 0; i < ngs.length; i++) {
            NG ng = ngs[i];
            reversed[i] = new NG(ng.n);
            reversed[i].depth = ng.depth;
            reversed[i].nearest = new NG[0];

        }

        for (int i = 0; i < ngs.length; i++) {
            NG at = ngs[i];
            for (int j = 0; j < at.nearest.length; j++) {
                NG next = reversed[(int) at.nearest[j].n.id];
                NG[] grow = Arrays.copyOf(next.nearest, next.nearest.length + 1);
                grow[grow.length - 1] = reversed[i];
                next.nearest = grow;
            }
        }

        System.out.println();


        for (NG n : reversed) {
            n.asString();

        }
        System.out.println();


        System.out.println();
//
        Map<Long, Integer> tos = Maps.newHashMap();
        for (NG n : ngs) {
            tos.compute(n.n.id, (k, v) -> v == null ? 1 : v + 1);
            for (int i = 0; i < n.nearest.length; i++) {
                tos.compute(n.nearest[i].n.id, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        Set<Integer> starts = Sets.newHashSet();
        for (Entry<Long, Integer> entry : tos.entrySet()) {
            if (entry.getValue() == 1) {
                starts.add((int) (long) entry.getKey());
            }
        }


        System.out.println();
//
        System.out.println(starts);
        System.out.println(ends);
//
//        System.out.println();
        System.out.println(starts.size() + " " + ngs.length);


        long naiveElapse = 0;
        long fancyElapse = 0;
        N naiveFound = null;
        NG fancyFound = null;
        N query = null;

        AtomicLong naiveCount = new AtomicLong();
        AtomicLong fancyCount = new AtomicLong();


        for (int j = 0; j < 1; j++) {


            query = randomN(rand, -1, numFeatures);
            long timestamp = System.currentTimeMillis();
            naiveFound = naive(query, neighbors);
            naiveCount.addAndGet(neighbors.length);
            naiveElapse = System.currentTimeMillis() - timestamp;

            timestamp = System.currentTimeMillis();

            NG start = null;
            NG winner = null;
            double best = Double.MAX_VALUE;
            for (Integer integer : ends) {
                start = reversed[integer];

                double ed = euclidianDistance(start.n.features, query.features, -1);
                fancyCount.incrementAndGet();
                if (ed < best) {
                    best = ed;
                    winner = start;


                    fancyFound = null;
                    do {
                        if (fancyFound != null) {
                            start = fancyFound;
                        }
                        fancyFound = start.find(query, fancyCount, ends);
                        if (fancyFound != null) {
                            System.out.println(start.n.id + "->" + fancyFound.n.id + ";");
                        }
                    }
                    while (fancyFound != start);

                    ed = euclidianDistance(fancyFound.n.features, query.features, -1);
                    if (ed < best) {
                        best = ed;
                        winner = fancyFound;
                    }
                }
            }
            fancyFound = winner;

            fancyElapse += System.currentTimeMillis() - timestamp;
        }

        System.out.println(
            "fancy:" + fancyCount.get() + " " + fancyElapse + " | " + euclidianDistance(fancyFound.n.features, query.features, -1) + " answer:" + fancyFound.n);
        System.out.println(
            "naive:" + naiveCount.get() + " " + naiveElapse + " | " + euclidianDistance(naiveFound.features, query.features, -1) + " answer:" + naiveFound);
        System.out.println("query:" + query);
        System.out.println("seed:" + seed);

    }

    static int[] topN(int i, N[] neighbors, int topN) {
        N n = neighbors[i];
        Comparator<TN> comparator = (o1, o2) -> Double.compare(o2.v, o1.v);
        MinMaxPriorityQueue<TN> heap = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(topN).create();
        for (int j = 0; j < neighbors.length; j++) {
            if (j != i) {
                heap.add(new TN((int) neighbors[j].id, euclidianDistance(n.features, neighbors[j].features, -1)));
            }
        }
        int[] is = new int[topN];
        int j = 0;
        List<TN> all = Lists.newArrayList(heap);
        Collections.sort(all, comparator);
        for (TN tn : all) {
            is[j] = tn.i;
            j++;
        }
        return is;
    }

    static class TN {
        private final int i;
        private final double v;

        TN(int i, double v) {
            this.i = i;
            this.v = v;
        }
    }


    public static N randomN(Random random, long id, int numFeatures) {
        double[] features = new double[numFeatures];
        for (int i = 0; i < features.length; i++) {
            features[i] = random.nextDouble();
        }
        return new N(id, features);
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
