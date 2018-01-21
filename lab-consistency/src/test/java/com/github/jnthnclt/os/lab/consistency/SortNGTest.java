package com.github.jnthnclt.os.lab.consistency;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SortNGTest {

    @Test
    public void testQI() throws Exception {
        int count = 1000;
        long[] longs = new long[count];
        int[] ints = new int[count];

        for (int i = 0; i < count; i++) {
            longs[i] = i;
            ints[i] = i;
        }
        shuffleArray(longs);
        shuffleArray(ints);

        long[] lcopy = Arrays.copyOfRange(longs,0,longs.length);
        int[] icopy = Arrays.copyOfRange(ints,0,ints.length);

        Sort.sortLI(longs, ints, 0, count);

        for (int j = 0; j < count; j++) {
            long l = longs[j];
            int i = ints[j];

            for (int k = 0; k < count; k++) {
                if (lcopy[k] == l) {
                    Assert.assertEquals(icopy[k],i);
                    break;
                }
            }
        }
    }

    // Fisher–Yates shuffle
    static void shuffleArray(int[] ar) {
        Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            int a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    static void shuffleArray(long[] ar) {
        Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            long a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }
}
