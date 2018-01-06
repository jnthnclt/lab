package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class QuorumIndex {

    /**
     * Takes a list of timestamps and returns the set of indexs that comprise a quorum or null
     *
     * @param timestamps
     * @return
     */
    static public int[] qi(long[] timestamps) {
        if (timestamps == null || timestamps.length < 3) {
            return null;
        }

        int replication = timestamps.length;
        int[] keys = new int[replication];
        for (int i = 0; i < replication; i++) {
            keys[i] = i;
        }
        long[] copyOfTimestamps = new long[replication];
        System.arraycopy(timestamps, 0, copyOfTimestamps, 0, replication);
        Sort.sortLI(copyOfTimestamps, keys, 0, replication);

        int quorum = (replication / 2) + 1;
        int q = 1;
        int qi = 0;
        long t = copyOfTimestamps[0];

        for (int i = 1; i < replication; i++) {
            if (t > -1 && t == copyOfTimestamps[i]) {
                q++;
            } else {
                if (q < quorum) {
                    qi = i;
                    q = 1;
                    t = copyOfTimestamps[i];
                } else {
                    break;
                }
            }
        }
        int[] answer = null;
        if (q >= quorum) {
            answer = new int[q];
            System.arraycopy(keys, qi, answer, 0, q);
        }

        return answer;
    }


}
