package com.github.jnthnclt.os.lab.nn;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class LABGG {


    public static void main(String[] args) {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        int numFeatures = 3;
        int numN = 81;

        P[] neighbors = new P[numN];
        Map<Integer, NP> lut = Maps.newHashMap();
        int id = 0;
        for (int i = 0; i < numN; i++) {
            P n = P.random(rand, id, numFeatures);
            neighbors[i] = n;
            lut.put(id, new NP(n));
            id++;
        }

        int px = 0;
        int size = (int)Math.sqrt(numN);
        System.out.println(size);
        for (int x = 0; x < size; x++) {
            for (int y = 0; y < size; y++) {
                neighbors[px].features[0] = x/size;
                neighbors[px].features[1] = y/size;
                px++;
            }
        }

        int depth = 1;
        while (true) {

            NP[] values = lut.values().toArray(new NP[0]);
            Map<Integer, NP> newlut = Maps.newHashMap();
            for (NP from : values) {

                NP forwardBest = null;
                double bfd = Double.MAX_VALUE;
                for (NP to : values) {
                    if (from != to) { // avoid self
                        double ed = NN.comparableEuclidianDistance(from.n.features, to.n.features);
                        if (ed < bfd) {
                            bfd = ed;
                            forwardBest = to;
                        }
                    }
                }

                if (forwardBest != null) {
                    NP backwardBest = null;
                    double bbd = Double.MAX_VALUE;
                    for (NP to : values) {
                        if (forwardBest != to) { // avoid self
                            double ed = NN.comparableEuclidianDistance(forwardBest.n.features, to.n.features);
                            if (ed < bbd) {
                                bbd = ed;
                                backwardBest = to;
                            }
                        }
                    }

                    if (backwardBest == from) {
                        //System.out.println(from.n.id + "<->" + forwardBest.n.id + "<->" + backwardBest.n.id);

                        if (lut.containsKey(from.n.id) && lut.containsKey(forwardBest.n.id)) {

                            lut.remove(from.n.id);
                            lut.remove(forwardBest.n.id);

                            double[] f = NN.avg(from.n.features, forwardBest.n.features);
                            P n = new P(id, f);
                            NP value = new NP(n);
                            value.depth = depth;
                            value.nearest = new NP[] { from, forwardBest };
                            newlut.put(id, value);
                            id++;
                        }
                    }
                }

            }
            depth++;

            size = newlut.size();
            System.out.println("lut:" + lut.size()+" pairs:" + size);
            lut.putAll(newlut);
            if (newlut.size() < 1) {
                break;
            }

        }

        NP start = lut.values().toArray(new NP[0])[0];


        System.out.println();
        start.dot();


        System.out.println();
        System.out.println("lut:" + lut.size());
        System.out.println();



        for (int i = 0; i < 100; i++) {


            long naiveElapse = 0;
            long fancyElapse = 0;
            P naiveFound = null;
            NP fancyFound = null;
            P query = null;

            AtomicLong naiveCount = new AtomicLong();
            AtomicLong fancyCount = new AtomicLong();

            query = P.random(rand, -1, numFeatures);
            long timestamp = System.currentTimeMillis();
            naiveFound = BruteForce.bruteForce(query, neighbors);
            naiveCount.addAndGet(neighbors.length);
            naiveElapse = System.currentTimeMillis() - timestamp;

            timestamp = System.currentTimeMillis();

            start = lut.values().toArray(new NP[0])[0];
            do {
                if (fancyFound != null) {
                    start = fancyFound;
                }
                fancyFound = start.find(query, fancyCount);
                if (fancyFound != null) {
                    //System.out.println(start.n.id + "->" + fancyFound.n.id + ";");
                }
            }
            while (fancyFound != start);
            fancyElapse += System.currentTimeMillis() - timestamp;


            System.out.println(
                "fancy:" + fancyCount.get() + " " + fancyElapse + " | " + NN.comparableEuclidianDistance(fancyFound.n.features, query.features) + " answer:" + fancyFound.n);
            System.out.println(
                "naive:" + naiveCount.get() + " " + naiveElapse + " | " + NN.comparableEuclidianDistance(naiveFound.features, query.features) + " answer:" + naiveFound);
            System.out.println("query:" + query);
            System.out.println("seed:" + seed);
        }
    }





    static class TN {
        private final int i;
        private final double v;

        TN(int i, double v) {
            this.i = i;
            this.v = v;
        }
    }





}
