package com.github.jnthnclt.os.lab.nn;

import com.google.common.collect.Maps;
import java.util.Map;

public class Agglomerative {

    static NP build(P[] neighbors) {
        int numN = neighbors.length;

        Map<Integer, NP> lut = Maps.newHashMap();
        int id = 0;
        for (int i = 0; i < numN; i++) {
            lut.put(id, new NP(neighbors[i]));
            id++;
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

            int size = newlut.size();
            System.out.println("lut:" + lut.size()+" pairs:" + size);
            lut.putAll(newlut);
            if (newlut.size() < 1) {
                break;
            }

        }

        return lut.values().toArray(new NP[0])[0];
    }
}
