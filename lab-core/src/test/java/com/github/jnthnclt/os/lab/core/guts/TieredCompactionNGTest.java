package com.github.jnthnclt.os.lab.core.guts;

import java.util.Random;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class TieredCompactionNGTest {

    @Test
    public void testBla() {
        long[] counts = new long[]{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
        for (long count : counts) {
            //System.out.println(Math.log(count));
        }
    }

    @Test
    public void testMergeRange() {

        int minimumRun = 4;
        //long[] counts = new long[]{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
        //long[] counts = new long[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        long[] counts = new long[0];

        int l = counts.length;
        boolean[] merging = new boolean[l];
        long[] generations = new long[l];
        Random rand = new Random();
        for (int a = 0; a < 100; a++) {

            if (merging.length >= minimumRun) {
                long solutionCost = 0;
                while (merging.length > 2) {

                    //System.out.println("Merge:" + Arrays.toString(counts) + "\t\t\t" + Arrays.toString(generations));
                    MergeRange mergeRange = TieredCompaction.hbaseSause(minimumRun, merging, counts, counts, generations);
                    if (mergeRange == null) {
                        break;
                    }
                    //System.out.println("Solution:" + TieredCompaction.range(counts, mergeRange.offset, mergeRange.length));

                    int length = (merging.length - mergeRange.length) + 1;
                    int nci = 0;
                    long[] newCounts = new long[length];
                    long[] newGenerations = new long[length];
                    boolean[] newMerging = new boolean[length];

                    for (int i = 0; i < counts.length; i++) {
                        if (i < mergeRange.offset) {
                            newCounts[nci] = counts[i];
                            newGenerations[nci] = generations[i];
                            nci++;
                        } else if (i > (mergeRange.offset + mergeRange.length - 1)) {
                            nci++;
                            newCounts[nci] = counts[i];
                            newGenerations[nci] = generations[i];
                            solutionCost += counts[i];
                        } else {
                            newGenerations[nci] = mergeRange.generation + 1;
                            newCounts[nci] += counts[i];
                        }
                    }
                    counts = newCounts;
                    generations = newGenerations;
                    merging = newMerging;
                    //System.out.println("----------------------");

                }
                //System.out.println("** Cost:" + solutionCost + "\n");
            }

            int nl = counts.length;
            long[] newCounts = new long[nl + 1];
            long[] newGenerations = new long[nl + 1];
            boolean[] newMerging = new boolean[nl + 1];

            int count = rand.nextInt(5000);
            //System.out.println("Preppending:" + count);

            newCounts[0] = count;
            newGenerations[0] = 0;
            newMerging[0] = false;

            System.arraycopy(counts, 0, newCounts, 1, nl);
            System.arraycopy(generations, 0, newGenerations, 1, nl);
            System.arraycopy(merging, 0, newMerging, 1, nl);

            counts = newCounts;
            generations = newGenerations;
            merging = newMerging;
        }
    }
}
