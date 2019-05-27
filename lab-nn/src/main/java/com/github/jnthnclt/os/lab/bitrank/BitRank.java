package com.github.jnthnclt.os.lab.bitrank;

import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.core.LABEnvironmentBuilder;
import com.github.jnthnclt.os.lab.core.LABHeapPressureBuilder;
import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.core.search.LABSearchIndex;
import com.github.jnthnclt.os.lab.core.search.LABSearchIndexUpdates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.RoaringBitmap;

public class BitRank {

    public static void main(String[] args) throws Exception {

        System.out.println(Integer.toBinaryString(0));
        System.out.println(Integer.toBinaryString(2));
        System.out.println(Integer.toBinaryString(4));


        int[] labels = MnistReader.getLabels("/jc/bitrank/train-labels-idx1-ubyte");
        List<int[][]> images = MnistReader.getImages("/jc/bitrank/train-images-idx3-ubyte");

        System.out.println(labels.length);


        LABSearchIndex index = buildTmpIndex();

        LABSearchIndexUpdates updates = new LABSearchIndexUpdates();

        int allOrdinal = index.fieldOrdinal("all");
        int labelOrdinal = index.fieldOrdinal("label");
        int idOrdinal = index.fieldOrdinal("id");

        long timestamp = 0;

        int id = 0;
        for (int[][] image : images) {

//            if (labels[id] == 9) {
//                print(image);
//            }

            updates.store(id, idOrdinal, timestamp, UIO.intBytes(id));
            updates.updateStrings(id, labelOrdinal, timestamp, String.valueOf(labels[id]));
            updates.updateStrings(id, allOrdinal, timestamp, String.valueOf("all"));


            for (String[] s : fingerprint(image, 0)) {
                int pixelOrdinal = index.fieldOrdinal(s[0]);
                updates.updateStrings(id, pixelOrdinal, timestamp, s[1]);
            }


            id++;
            if (id % 10000 == 0) {
                index.update(updates, false);
                updates.clear();
                System.out.println(id);
            }

        }

        index.update(updates, false);
        updates.clear();
        index.flush();
        System.out.println("Indexing Complete");

//        index.storedValues(index.bitmap(labelOrdinal, String.valueOf(2)), idOrdinal, (index1, value) -> {
//
//            int vi = value.getInt(0);
//            System.out.println(labels[vi]);
//            print(images.get(vi));
//
//            return true;
//        });

        // t10k-images-idx3-ubyte	t10k-labels-idx1-ubyte


        int[] testLabels = MnistReader.getLabels("/jc/bitrank/t10k-labels-idx1-ubyte");
        List<int[][]> testImages = MnistReader.getImages("/jc/bitrank/t10k-images-idx3-ubyte");


        List<String> allLabels = index.fieldStringValues(labelOrdinal, Integer.MAX_VALUE);
        Collections.sort(allLabels);

        Map<String, BitmapAndCount> cache = Maps.newHashMap();

        int idx = 0;
        int wrong = 0;
        for (int[][] image : testImages) {

            List<BitmapAndCount> all = Lists.newArrayList();
            for (String[] s : fingerprint(image, 0)) {
                String k = s[0] + ":" + s[1];

                int pixelOrdinal = index.fieldOrdinal(s[0]);
                for (String value : index.fieldStringValues(pixelOrdinal, Integer.MAX_VALUE)) {
//                    if (!s[1].equals(value)) {
//
//                    }
                    System.out.print(s[0] + " " + s[1] + " | ");
                    RoaringBitmap bitmap = index.bitmap(pixelOrdinal, s[1]);
                    if (bitmap == null) {
                        bitmap = new RoaringBitmap();
                    }
                    for (String l : allLabels) {
                        System.out.print(RoaringBitmap.and(index.bitmap(labelOrdinal, l), bitmap).getCardinality());
                        System.out.print(", ");
                    }
                    System.out.println();

                }


                BitmapAndCount got = cache.get(k);
                if (got == null) {

                    RoaringBitmap bitmap = index.bitmap(pixelOrdinal, s[1]);
                    if (bitmap != null) {
                        got = new BitmapAndCount(bitmap);
                    } else {
                        got = new BitmapAndCount(new RoaringBitmap());
                    }
                    cache.put(k, got);
                }
                if (got.count > 0) {
                    all.add(got);
                }
            }

            long start = System.currentTimeMillis();
            List<RoaringBitmap> condense = condense(all);
            long duration = System.currentTimeMillis() - start;

            int[] choices = new int[10];
            int[] lastChoice = new int[10];

            index.storedValues(condense.get(condense.size() - 1), idOrdinal, (index1, value) -> {

                int vi = value.getInt(0);
                choices[labels[vi]]++;
                lastChoice[labels[vi]] = vi;
                return true;
            });


            int max = 0;
            int maxi = 0;
            for (int i = 0; i < choices.length; i++) {
                if (choices[i] > max) {
                    max = choices[i];
                    maxi = i;
                }
            }

            double errorRate = wrong / (idx * 1.0);

            System.out.println(idx + " " + testLabels[idx] + " " + (testLabels[idx] == maxi) + " tally:" + Arrays.toString(
                choices) + " errors:" + errorRate + " millis:" + duration);
            if (testLabels[idx] != maxi) {
                print(image, images.get(lastChoice[maxi]));
                wrong++;
            }

            idx++;
        }
    }


    static List<RoaringBitmap> condense(List<BitmapAndCount> all) {

        Collections.sort(all);

        List<RoaringBitmap> counts = Lists.newArrayList();
        counts.add(new RoaringBitmap());
        for (BitmapAndCount a : all) {
            int i = 0;
            RoaringBitmap bitmap = a.bitmap;
            while (true) {
                RoaringBitmap inOnce = RoaringBitmap.and(counts.get(i), bitmap);
                counts.get(i).or(bitmap);
                if (inOnce != null && inOnce.getCardinality() > 0) {
                    i++;
                    if (counts.size() == i) {
                        counts.add(new RoaringBitmap());
                    }
                    bitmap = inOnce;
                } else {
                    break;
                }
            }
        }
        return counts;
    }

    static class BitmapAndCount implements Comparable<BitmapAndCount> {
        private final RoaringBitmap bitmap;
        private final int count;

        BitmapAndCount(RoaringBitmap bitmap) {
            this.bitmap = bitmap;
            this.count = bitmap.getCardinality();
        }

        @Override
        public int compareTo(BitmapAndCount o) {
            return -Integer.compare(count, o.count);
        }
    }

    private static List<String[]> fingerprint(int[][] image, int jitter) {

//        Set<String> sml = Sets.newHashSet();
//
//        sml.addAll(fingerprint_simple("S",image));
//        sml.addAll(fingerprint_simple("M",half(image)));
//        sml.addAll(fingerprint_simple("L",half(half(image))));
//
//        ArrayList<String> list = Lists.newArrayList(sml);
//        Collections.sort(list);
//        return list;

        return fingerprint_simple("", image, jitter);
    }

    private static List<String[]> fingerprint_simple(String prefix, int[][] image, int jitter) {

        //int[] centroid = centroid(image);

        List<String[]> fingerprint = Lists.newArrayList();
        for (int x = 0; x < image.length; x++) {
            for (int y = 0; y < image[x].length; y++) {

                String fieldName = prefix + "_" + x + "_" + y;
                int n = 0;
                if (image[x][y] > 0) {
                    n = (int) ((image[x][y] / 255.0) * 8) + 1;
                }
                fingerprint.add(new String[] { fieldName, String.valueOf(n) });
                if (n != 0 && jitter > 0) {
                    for (int i = Math.min(1, n - jitter); i < Math.max(9, n + jitter); i++) {
                        if (i != n) {
                            fingerprint.add(new String[] { fieldName, String.valueOf(i) });
                        }
                    }
                }
            }
        }
        return fingerprint;
    }

    private static int distance(int x, int y, int ox, int oy) {
        int dx = Math.abs(x - ox);
        int dy = Math.abs(y - oy);
        return (int) Math.sqrt((dy * dy) + (dx * dx));
    }

    private static int[][] half(int[][] image) {

        int[][] half = new int[image.length / 2][image[0].length / 2];

        for (int x = 0; x < image.length; x++) {
            for (int y = 0; y < image[x].length; y++) {
                half[x / 2][y / 2] += image[x][y];
            }
        }
        for (int x = 0; x < half.length; x++) {
            for (int y = 0; y < half[x].length; y++) {
                half[x][y] /= 2;
            }
        }
        return half;
    }

    private static void print(int[][] image) {

        System.out.println("w:" + image.length + " h:" + image[0].length);

        int[] centroid = centroid(image);


        for (int x = 0; x < image.length; x++) {
            for (int y = 0; y < image[x].length; y++) {

                if (centroid[0] == x && centroid[1] == y) {
                    System.out.print('X');
                } else {
                    if (image[x][y] > 0) {
                        int n = (int) ((image[x][y] / 255.0) * 8) + 1;
                        System.out.print(n);
                    } else {
                        System.out.print('0');
                    }
                }
            }
            System.out.println();
        }
    }

    private static void print(int[][] a, int[][] b) {

        System.out.println("w:" + a.length + " h:" + a[0].length);

        int[] ca = centroid(a);
        int[] cb = centroid(b);


        for (int x = 0; x < a.length; x++) {
            for (int y = 0; y < a[x].length; y++) {

                if (ca[0] == x && ca[1] == y) {
                    System.out.print('X');
                } else {
                    if (a[x][y] > 0) {
                        int n = (int) ((a[x][y] / 255.0) * 8) + 1;
                        System.out.print(n);
                    } else {
                        System.out.print('-');
                    }
                }
            }

            System.out.print("   ");

            for (int y = 0; y < b[x].length; y++) {

                if (cb[0] == x && cb[1] == y) {
                    System.out.print('X');
                } else {
                    if (b[x][y] > 0) {
                        int n = (int) ((b[x][y] / 255.0) * 8) + 1;
                        System.out.print(n);
                    } else {
                        System.out.print('-');
                    }
                }
            }


            System.out.print("   ");

            for (int y = 0; y < b[x].length; y++) {

                if (a[x][y] > 0 && b[x][y] > 0) {
                    System.out.print('-');
                } else if (a[x][y] == 0 && b[x][y] == 0) {
                    System.out.print('-');
                } else {
                    System.out.print('1');
                }

            }

            System.out.println();
        }
    }

    private static int[] centroid(int[][] image) {

        int cx = 0;
        int cy = 0;

        int c = 0;
        for (int x = 0; x < image.length; x++) {
            for (int y = 0; y < image[x].length; y++) {
                if (image[x][y] > 0) {
                    cx += x;
                    cy += y;
                    c++;
                }
            }
        }

        return new int[] { cx / c, cy / c };
    }


    private static LABSearchIndex buildTmpIndex() throws Exception {
        File root = Files.createTempDir();
        System.out.println(root.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABFiles labFiles = new LABFiles();

        LABHeapPressureBuilder labHeapPressureBuilder = new LABHeapPressureBuilder(globalHeapCostInBytes);
        LABEnvironmentBuilder labEnvironmentBuilder = new LABEnvironmentBuilder().setLABFiles(labFiles);
        LABIndexProvider<byte[]> labIndexProvider = new LABIndexProvider<>(stats,
                labHeapPressureBuilder,
                labEnvironmentBuilder);
        return new LABSearchIndex(labIndexProvider, root);
    }


}
