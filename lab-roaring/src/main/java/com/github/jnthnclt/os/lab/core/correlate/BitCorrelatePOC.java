package com.github.jnthnclt.os.lab.core.correlate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class BitCorrelatePOC {

    static int range = Integer.MAX_VALUE;

    enum Field {
        fieldA(1, range);

        int min;
        int max;
        Random random = new Random();

        Field(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public float generate() {
            return (min + random.nextInt(max)) / (float) max;
        }
    }


    /*
     * Borrowing from the BitSort concept lets see if we can reduce the problem space around n^2 correlations
     */
    public static void main(String[] args) {

        int fieldCount = 32;

        float sensitivity = 0.1f;


        int count = 2_000_000;
        int leafsize = Short.MAX_VALUE * 4;


        Index index = new Index();
        Field[] fields = new Field[fieldCount];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = Field.fieldA;
        }

        for (int i = 0; i < count; i++) {
            index.add(i, buildVec(fields));
            if (i % 500_000 == 0) {
                System.out.println(i);
            }
        }

        BitSortTreePOC[] trees = new BitSortTreePOC[fields.length];
        for (int i = 0; i < fields.length; i++) {

            List<IV> input = Lists.newArrayList();
            for (Entry<Integer, float[]> entry : index.idToDocVec.entrySet()) {
                int dex = index.idToIndex.get(entry.getKey());
                input.add(new IV(dex, entry.getValue()[i]));
            }

            long start = System.currentTimeMillis();
            BitSortTreePOC tree = new BitSortTreePOC();
            String stats = tree.populate(input, leafsize);
            tree.bitSort.link();
            System.out.println(i + " BitSortTree elapse:" + (System.currentTimeMillis() - start) + " " + stats);
            trees[i] = tree;
        }


        for (int i = 0; i < 10; i++) {

            float[] query = buildVec(fields);

            // Brute Force
            long start = System.nanoTime();
            int id = -1;
            double best = Double.MAX_VALUE;
            for (int dex : index.idToDocVec.keySet()) {
                float[] value = index.idToDocVec.get(dex);
                double dist = distance(query, value);
                if (dist < best) {
                    best = dist;
                    id = dex;
                }
            }
            long bruteNanos = System.nanoTime() - start;
            float[] bruteVec = index.idToDocVec.get(id);

            System.out.println("\n\nquery:" + i);

            start = System.nanoTime();
            float[] braunVec = index.correlate(start, query, trees, 0, 10, sensitivity);
            long braunNanos = System.nanoTime() - start;
            if (!Arrays.equals(bruteVec, braunVec)) {
                System.out.println("query:" + Arrays.toString(query));
                System.out.println("brute:" + Arrays.toString(bruteVec));
                System.out.println("braun:" + Arrays.toString(braunVec));
                System.out.println("!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("!! WTF WTF WTF WTF !!");
                System.out.println("!!!!!!!!!!!!!!!!!!!!!");
            }
            long diff = bruteNanos - braunNanos;
            if (diff < 0) {
                System.out.println(
                        (float) (((braunNanos / (double) bruteNanos)) ) + "x slower by " + (diff / 1000000d) + " millis  brute:" + (bruteNanos / 1000000d) + " braun:" + (braunNanos / 1000000d));
            } else {
                System.out.println(
                        (float) (((bruteNanos / (double) braunNanos)) ) + "x faster by " + (diff / 1000000d) + " millis  brute:" + (bruteNanos / 1000000d) + " braun:" + (braunNanos / 1000000d));
            }
        }

    }

    static class IV {
        final int id;
        final float value;

        IV(int id, float value) {
            this.id = id;
            this.value = value;
        }
    }

    static float[] buildVec(Field[] fields) {
        float[] vec = new float[fields.length];
        for (int i = 0; i < fields.length; i++) {
            vec[i] = fields[i].generate();
        }
        return vec;
    }


    static class Index {
        Map<Integer, Integer> idToIndex = Maps.newHashMap();
        Map<Integer, Integer> indexToId = Maps.newHashMap();
        Map<Integer, float[]> idToDocVec = Maps.newHashMap();
        AtomicInteger idx = new AtomicInteger();

        public void add(int id, float[] values) {
            Integer index = idToIndex.computeIfAbsent(id, integer -> idx.getAndIncrement());
            indexToId.put(index, id);
            idToDocVec.put(index, values);
        }


        public float[] correlate(long start,
                                 float[] query,
                                 BitSortTreePOC[] sortTrees,
                                 int offset,
                                 int limit,
                                 float sensitivity) {

            double[] features_min_value = new double[query.length];
            double[] features_max_value = new double[query.length];
            Arrays.fill(features_max_value, range); // hacky


            FeatureBits[] features = new FeatureBits[query.length];

            for (int j = 0; j < query.length; j++) {
                BitSortTreePOC sortTree = sortTrees[j];
                FloatBitSort leaf = sortTree.bitSort.leaf(query[j]);
                features[j] = new FeatureBits(j, leaf);
            }

            double best = Double.MAX_VALUE;
            int expanded = 1;
            int pass = 0;
            int evaled = 0;
            float[] bestDocVec = null;

            // AndAdd andAdd = new AndAdd(query.length);

            //RoaringBitmap accum = new RoaringBitmap();

            while (expanded > 0) {

                RoaringBitmap candidate = null;
                for (FeatureBits feature : features) {
                    candidate = feature.collect(candidate);
                    if (candidate.getCardinality() == 0) {
                        break;
                    }
                }

                RoaringBitmap accum = candidate;


                int cardinality = accum.getCardinality();
                if (cardinality > 0) {
                    evaled += cardinality;

                    double oldBest = best;
                    for (Integer index : accum) {
                        Integer id = indexToId.get(index);
                        float[] documentVec = idToDocVec.get(id);
                        double dist = distance(query, documentVec);
                        if (dist < best) {
                            //found = true;
                            best = dist;
                            bestDocVec = documentVec;
                        }
                    }


                    if (oldBest > best) {
                        bounds(query, best, features_min_value, features_max_value, sensitivity);
                        System.out.println(
                                pass + ": " + best + " evaled:" + evaled + " elapseNanos:" + ((System.nanoTime() - start)) / 1000000d);

//                        for (int i = 0; i < features.length; i++) {
//                            if (features_max_value[i] < 1 && features_min_value[i] > 0 && features_max_value[i] - features_min_value[i] < 1) {
//                                System.out.println(i + " " + features_min_value[i] + ".." + features_max_value[i]);
//                            }
//                        }

                    }

//                    if (bestDocVec != null) {
//                        Arrays.sort(features, (o1, o2) -> {
//                            double o1r = features_max_value[o1.id] - features_min_value[o1.id];
//                            double o2r = features_max_value[o2.id] - features_min_value[o2.id];
//                            return Double.compare(o1r,o2r);
//                        });
//                    }
                    accum.clear();
                }

                //System.out.println(features_max_value[features[0].id] - features_min_value[features[0].id]);

                expanded = 0;
                for (int i = 0; i < features.length; i++) {
                    FeatureBits feature = features[i];
                    feature.subtract(candidate);
                    if (feature.expand(features_min_value[i], features_max_value[i], false)) {
                        expanded++;
                    }
                }


                pass++;
            }
            // System.out.println("Slow Exit after:" + pass + " elapseNanos:" + ((System.nanoTime() - start)) / 1000000d);
            return bestDocVec;
        }

        // Euclidain
//        public void bounds(float[] query,
//                           double best,
//                           double[] us,
//                           double[] ls,
//                           float sensitivity) {
//
//            double k = best;
//            double sqrt = Math.sqrt(k);
//
//            for (int i = 0; i < query.length; i++) {
//                double u = query[i] - (sqrt * sensitivity);
//                double l = query[i] + (sqrt * sensitivity);
//                us[i] = u;
//                ls[i] = l;
//            }
//
//        }

        // Manhatten
        public void bounds(float[] query,
                           double best,
                           double[] us,
                           double[] ls,
                           float sensitivity) {

            double k = best;
            for (int i = 0; i < query.length; i++) {
                us[i] = Math.max(0,query[i] - (k*sensitivity));
                ls[i] = Math.min(Integer.MAX_VALUE,query[i] + (k*sensitivity));
            }

        }
    }


    static class FeatureBits {
        private final int id;
        FloatBitSort smaller;
        FloatBitSort bigger;
        final RoaringBitmap bitmap;

        FeatureBits(int id, FloatBitSort bits) {
            this.id = id;
            this.smaller = bits;
            this.bigger = bits;
            this.bitmap = bits.bitmap().clone();
        }

        public boolean expand(double min, double max, boolean expand_to_bounds) {
            boolean expanded = false;
            if (smaller != null || bigger != null) {


                if (smaller != null && smaller.smaller != null && smaller.smaller.maxValue > min) {

                    if (expand_to_bounds) {
                        do {
                            bitmap.or(smaller.smaller.bitmap());
                            smaller = smaller.smaller;
                            expanded = true;
                        } while (smaller != null && smaller.smaller != null && smaller.maxValue > min);

                    } else {

                        bitmap.or(smaller.smaller.bitmap());
                        smaller = smaller.smaller;
                        expanded = true;
                    }
                }
                if (bigger != null && bigger.bigger != null && bigger.bigger.minValue < max) {
                    if (expand_to_bounds) {
                        do {
                            bitmap.or(bigger.bigger.bitmap());
                            bigger = bigger.bigger;
                            expanded = true;
                        } while (bigger != null && bigger.bigger != null && bigger.minValue < max);
                    } else {
                        bitmap.or(bigger.bigger.bitmap());
                        bigger = bigger.bigger;
                        expanded = true;
                    }
                }
            }
            return expanded;
        }

        public RoaringBitmap getBitmap() {
            return bitmap;
        }

        public RoaringBitmap collect(RoaringBitmap collect) {
            if (collect == null) {
                collect = bitmap.clone();
            } else {
                collect.and(bitmap);
            }
            return collect;
        }

        public void subtract(RoaringBitmap subtract) {
            bitmap.andNot(subtract);
        }

    }

    static class AndAdd {
        RoaringBitmap[] counts;

        public AndAdd(int counts) {
            this.counts = new RoaringBitmap[counts];
            for (int i = 0; i < counts; i++) {
                this.counts[i] = new RoaringBitmap();
            }
        }

        public RoaringBitmap add(RoaringBitmap add) {
            RoaringBitmap and = null;
            for (int i = 0; i < counts.length; i++) {
                and = RoaringBitmap.and(counts[i], add);
                counts[i].or(add);
                if (and.getCardinality() == 0) {
                    break;
                }
                //add = RoaringBitmap.andNot(add,and);
            }
            return and;
        }
    }

    // Manhatten
    public static float distance(float[] docVector1, float[] docVector2) {
        float distance = 0;
        for (int i = 0; i < docVector1.length; i++) {
            distance += Math.abs(docVector1[i] - docVector2[i]);
        }
        return distance;
    }

    // Euclidian
//    public static float distance(float[] docVector1, float[] docVector2) {
//        float distance = 0;
//        for (int i = 0; i < docVector1.length; i++) {
//            float d = docVector1[i] - docVector2[i];
//            distance += d * d;
//        }
//        return distance;
//    }


    static class BitSortTreePOC {

        FloatBitSort bitSort;

        public String populate(List<IV> indexFieldValues, int leafSize) {

            long start = System.currentTimeMillis();
            Collections.sort(indexFieldValues, (o1, o2) -> {
                int c = Float.compare(o1.value, o2.value);
                if (c != 0) {
                    return c;
                }
                return Integer.compare(o1.id, o2.id);
            });
            long sortD = (System.currentTimeMillis() - start);

            int count = indexFieldValues.size();
            bitSort = new FloatBitSort(count, leafSize);

            start = System.currentTimeMillis();
            int i = 0;
            for (IV indexFieldValue : indexFieldValues) {
                bitSort.add(i, indexFieldValue.id, indexFieldValue.value);
                i++;
            }
            bitSort.done();
            return "sort:" + sortD + " index:" + (System.currentTimeMillis() - start);
        }

    }



}
