package com.github.jnthnclt.os.lab.core.guts;

/**
 *
 * @author jonathan.colt
 */
public class TieredCompaction {

    /*

    http://www.ngdata.com/visualizing-hbase-flushes-and-compactions/

    The algorithm is basically as follows:

    Run over the set of all store files, from oldest to youngest

    If there are more than 3 (hbase.hstore.compactionThreshold) store
    files left and the current store file is 20% larger then the sum of
    all younger store files, and it is larger than the memstore flush size,
    then we go on to the next, younger, store file and repeat step 2.

    Once one of the conditions in step two is not valid anymore, the store
    files from the current one to the youngest one are the ones that will
    be merged together. If there are less than the compactionThreshold,
    no merge will be performed. There is also a limit which prevents more
    than 10 (hbase.hstore.compaction.max) store files to be merged in one compaction.

     */
    public static MergeRange hbaseSause(int minimumRun, boolean[] mergingCopy, long[] indexCounts, long[] indexSizes, long[] generations) {

        if (minimumRun < 3 && mergingCopy.length > 1) {
            long g = 0;
            for (long generation : generations) {
                g = Math.max(g, generation);
            }
            return new MergeRange(g, 0, mergingCopy.length);
        }

        int maxMergedAtOnce = 10;

        for (int i = mergingCopy.length - 1; i > -1; i--) {

            if (mergingCopy[i]) {
                return null;
            }
            long oldSum = indexSizes[i];
            long g = generations[i];

            long youngSum = 0;
            int l = 2;
            int j = i - 1;
            for (; j > -1 && l < maxMergedAtOnce; j--, l++) {
                if (mergingCopy[j]) {
                    return null;
                }
                youngSum += indexSizes[j];
                g = Math.max(g, generations[j]);

                if ((l >= minimumRun || j == 0) && youngSum > (oldSum * 1.20d)) {
                    return new MergeRange(g, j, l);
                }
            }
        }
        return null;
    }

    public static String range(long[] counts, int offset, int l) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < offset; i++) {
            sb.append(counts[i]);
            sb.append(", ");
        }
        sb.append("[");
        for (int i = offset; i < offset + l; i++) {
            if (i > offset) {
                sb.append(", ");
            }
            sb.append(counts[i]);
        }
        sb.append("]");
        for (int i = offset + l; i < counts.length; i++) {
            sb.append(", ");
            sb.append(counts[i]);
        }
        sb.append(']');
        return sb.toString();
    }
}
