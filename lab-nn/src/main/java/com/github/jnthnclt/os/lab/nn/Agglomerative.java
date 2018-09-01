package com.github.jnthnclt.os.lab.nn;

import com.github.jnthnclt.os.lab.nn.weiszfeld.Input;
import com.github.jnthnclt.os.lab.nn.weiszfeld.Output;
import com.github.jnthnclt.os.lab.nn.weiszfeld.Point;
import com.github.jnthnclt.os.lab.nn.weiszfeld.WeightedPoint;
import com.github.jnthnclt.os.lab.nn.weiszfeld.WeiszfeldAlgorithm;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Agglomerative {


    static NP build(P[] neighbors, Distance distance) {
        int numN = neighbors.length;

        Map<Integer, NP> lut = Maps.newHashMap();
        int id = 0;
        for (; id < numN; id++) {
            lut.put(id, new NP(id, neighbors[id], 0));
        }
        id++;


        int depth = 1;
        while (lut.size() > 1) {

            System.out.print(depth + " " + lut.size()+" ");
            Map<Integer, NP> newStarts = Maps.newHashMap();
            NP[] all = lut.values().toArray(new NP[0]);
            List<Nearest> nearests = Lists.newArrayList();
            for (NP r : all) {
                Nearest nearest = nearest(r, all, distance);
                if (nearest != null) {
                    nearests.add(nearest);
                }
            }

            Collections.sort(nearests, (o1, o2) -> {
                int c = Double.compare(o1.distance, o2.distance);
                if (c == 0) {
                    return Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
                }
                return c;
            });


            Map<Integer, NP> newLut = Maps.newHashMap();
            for (Nearest nearest : nearests) {

                NP fp = nearest.fp;
                NP tp = nearest.tp;

                if (lut.containsKey(fp.id) && lut.containsKey(tp.id)) {
                    lut.remove(fp.id);
                    lut.remove(tp.id);

                    double[] f = centroid(fp, tp);
                    NP newNP = new NP(id, new P(f), depth);
                    newNP.nearest = new NP[] { fp, tp };
                    newStarts.put(id, newNP);
                    id++;

                    newLut.put(fp.id, newNP);
                    newLut.put(tp.id, newNP);

                }
            }
            for (Nearest nearest : nearests) {

                NP fp = nearest.fp;
                NP tp = nearest.tp;
                if (newLut.containsKey(fp.id) && lut.containsKey(tp.id)) {
                    lut.remove(tp.id);

                    NP newNP = newLut.get(fp.id);

                    int tl = newNP.nearest.length;
                    newNP.nearest = Arrays.copyOf(newNP.nearest, tl + 1);
                    newNP.nearest[tl] = tp;

                    double[] f = centroid(newNP.nearest);
                    newNP.p.features = f;

                } else if (lut.containsKey(fp.id) && newLut.containsKey(tp.id)) {
                    lut.remove(fp.id);

                    NP newNP = newLut.get(tp.id);

                    int tl = newNP.nearest.length;
                    newNP.nearest = Arrays.copyOf(newNP.nearest, tl + 1);
                    newNP.nearest[tl] = fp;

                    double[] f = centroid(newNP.nearest);
                    newNP.p.features = f;
                }
            }

            System.out.println(lut.size());

            depth++;
            lut = newStarts;
        }

        return lut.values().toArray(new NP[0])[0];
    }

    static class Nearest {
        public final NP fp;
        public final NP tp;
        public final double distance;


        Nearest(NP fp, NP tp, double distance) {
            this.fp = fp;
            this.tp = tp;
            this.distance = distance;
        }
    }

    static Nearest nearest(NP fnp, NP[] tnps, Distance distance) {
        NP nearest = null;
        double shortestDistance = Double.MAX_VALUE;

        double[] accumD = new double[] { 0 };
        int[] count = new int[] { 0 };

        for (int i = 0; i < tnps.length; i++) {
            NP tnp = tnps[i];
            if (fnp.id != tnp.id ) {

                accumD[0] = 0;
                count[0] = 0;

                fnp.leafs(f -> {
                    tnp.leafs((t) -> {
                        double d = distance.distance(f.features, t.features);
                        if (d >= 0) {
                            accumD[0] += d;
                            count[0]++;
                        }
                    });
                });

//                tnp.leafs((t) -> {
//                    double d = distance.distance(fnp.p.features, t.features);
//                    if (d >= 0) {
//                        accumD[0] += d;
//                        count[0]++;
//                    }
//                });


                double avgD = accumD[0] / (double) count[0];
                if (avgD < shortestDistance) {
                    shortestDistance = avgD;
                    nearest = tnp;
                }
            }
        }

        return (nearest == null) ? null : new Nearest(fnp, nearest, shortestDistance);

    }


    static double[] centroid(NP... as) {


//        int c = 0;
//        double[] features = new double[as[0].n.features.length];
//        for (NP a : as) {
//            NN.sum(features, a.n.features);
//            c++;
//            for (NP np : a.nearest) {
//                NN.sum(features, np.n.features);
//                c++;
//            }
//        }
//
//        for (int i = 0; i < features.length; i++) {
//            features[i] /= (double) c;
//        }
//
//        return features;

        List<WeightedPoint> wPoints = Lists.newArrayList();
        for (NP a : as) {
            wPoints.add(new WeightedPoint(new Point(a.p.features), 1));
            for (NP np : a.nearest) {
                wPoints.add(new WeightedPoint(new Point(np.p.features), 1));
            }
        }


        Input input = new Input();
        input.setDimension(as[0].p.features.length);
        input.setPoints(wPoints);
        input.setPermissibleError(0.00001);

        Output output = WeiszfeldAlgorithm.process(input);

        Point result = output.getPoint();
        return result.getValues();
    }
}
