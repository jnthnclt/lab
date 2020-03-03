package com.github.jnthnclt.os.lab.core.correlate;

import java.util.Arrays;
import java.util.Random;

import static com.github.jnthnclt.os.lab.core.correlate.BitCorrelatePOC.distance;

public class BitCorrelateEuclidianLimitsPOC {

    public static void main(String[] args) {
        Random r = new Random();
        int bounds = 100;

        float[] query = new float[]{r.nextInt(bounds), r.nextInt(bounds), r.nextInt(bounds)};
        float[] best = new float[]{r.nextInt(bounds), r.nextInt(bounds), r.nextInt(bounds)};

        double d = distance(query, best);


        double[] mins = new double[]{bounds, bounds, bounds};
        double[] maxs = new double[]{0.0, 0.0, 0.0};

        for (int i = -bounds; i < 2 * bounds; i++) {
            for (int j = -bounds; j < 2 * bounds; j++) {
                for (int k = -bounds; k < 2 * bounds; k++) {

                    double dist = distance(query, new float[]{i, j, k});
                    if (dist < d) {
                        mins[0] = Math.min(i, mins[0]);
                        mins[1] = Math.min(j, mins[1]);
                        mins[2] = Math.min(k, mins[2]);

                        maxs[0] = Math.max(i, maxs[0]);
                        maxs[1] = Math.max(j, maxs[1]);
                        maxs[2] = Math.max(k, maxs[2]);
                    }
                }
            }
        }

        System.out.println("d=" + d);


        System.out.println("Brute force:");

        System.out.println("mins:" + Arrays.toString(mins));
        System.out.println("query:" + Arrays.toString(query) + " at:" + Arrays.toString(best));
        System.out.println("maxs:" + Arrays.toString(maxs));


        // ((x-a)^2) + j = k
        // x = a - sqrt(k-j)
        // x = a + sqrt(k-j)

        int featureCount = 128;

        query = new float[featureCount];
        best = new float[featureCount];

        int jitter = 20;


        for (int i = 0; i < featureCount; i++) {
            query[i] = r.nextInt(bounds);
            best[i] = query[i] + (r.nextInt(jitter)-(jitter/2));
        }
        System.out.println("Diff:"+ distance(query, best)/featureCount/2);


        float[] us = new float[featureCount];
        float[] ls = new float[featureCount];

        float k = distance(query, best);
        float sqrt = (float) Math.sqrt(k);

        for (int i = 0; i < query.length; i++) {
            float u = query[i] - sqrt;
            float l = query[i] + sqrt;
            us[i] = u;
            ls[i] = l;
        }

        for (int i = 0; i < query.length; i++) {
            float u = query[i] - sqrt;
            float l = query[i] + sqrt;
            us[i] = Math.max(0,Math.min(u, us[i]));
            ls[i] = Math.min(bounds,Math.max(l, ls[i]));


        }

//        System.out.println("Bounds:");
//        System.out.println(Arrays.toString(min_bounds));
//        System.out.println(Arrays.toString(query));
//        System.out.println(Arrays.toString(max_bounds));


        System.out.println("\nMaths:");

        System.out.println(Arrays.toString(us));
        //System.out.println("query:" + Arrays.toString(query) + " at:" + Arrays.toString(best));
        System.out.println(Arrays.toString(ls));
    }
}
