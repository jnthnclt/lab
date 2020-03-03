package com.github.jnthnclt.os.lab.core.correlate;

import java.util.Arrays;
import java.util.Random;

public class BitCorrelateManhattenLimitsPOC {

    public static void main(String[] args) {
        Random r = new Random();
        int bounds = 100;

        float[] query = new float[]{r.nextInt(bounds), r.nextInt(bounds), r.nextInt(bounds)};
        float[] best = new float[]{r.nextInt(bounds), r.nextInt(bounds), r.nextInt(bounds)};

        double d = distance(query, best);

        double[] mins = new double[]{bounds, bounds, bounds};
        double[] maxs = new double[]{-bounds, -bounds, -bounds};

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

        System.out.println("Brute force:");
        System.out.println("mins:" + Arrays.toString(mins));
        System.out.println("query:" + Arrays.toString(query) + " at:" + Arrays.toString(best)+" "+distance(query,best));
        System.out.println("maxs:" + Arrays.toString(maxs));


        float[] us = new float[query.length];
        float[] ls = new float[query.length];

        // |y-x|+p=k
        // y = p - k + x
        // y = -p + k + x

        float k = distance(query, best);
        for (int i = 0; i < query.length; i++) {
            us[i] = query[i] - k;
            ls[i] = query[i] + k;
        }


        System.out.println("\nMaths:");

        System.out.println(Arrays.toString(us));
        //System.out.println("query:" + Arrays.toString(query) + " at:" + Arrays.toString(best));
        System.out.println(Arrays.toString(ls));
    }

    public static float distance(float[] docVector1, float[] docVector2) {
        float distance = 0;
        for (int i = 0; i < docVector1.length; i++) {
            distance += Math.abs(docVector1[i] - docVector2[i]);
        }
        return distance;
    }
}
