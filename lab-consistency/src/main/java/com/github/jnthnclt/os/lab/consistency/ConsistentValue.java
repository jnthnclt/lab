package com.github.jnthnclt.os.lab.consistency;

import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ConsistentValue<V> {
    private final long[] offeredTimestamps;
    private final Object[] offeredValues;

    private final long[] commitedTimestamps;
    private final Object[] commitedValues;

    ConsistentValue(int replication) {
        this.offeredTimestamps = new long[replication];
        Arrays.fill(this.offeredTimestamps, -1);
        this.offeredValues = new Object[replication];
        Arrays.fill(this.offeredValues, null);

        this.commitedTimestamps = new long[replication];
        Arrays.fill(this.commitedTimestamps, -1);
        this.commitedValues = new Object[replication];
        Arrays.fill(this.commitedValues, null);
    }

    public boolean set(int replicaId,
        @Nullable ValueTimestamp<V> expected,
        @Nonnull ValueTimestamp<V> desired,
        @Nonnull ValuesEqual<V> valuesEqual) {

        ValueTimestamp<V>[] accepted = null;
        synchronized (this) {
            if (expected != null) {
                int[] qi = quorumIndexSet(commitedTimestamps);
                if (qi != null
                    && commitedTimestamps[qi[0]] == expected.timestamp
                    && valuesEqual.equal((V) commitedValues[qi[0]], expected.value)) {

                    if (desired.timestamp > offeredTimestamps[replicaId]) {
                        offeredValues[replicaId] = desired.value;
                        offeredTimestamps[replicaId] = desired.timestamp;
                        accepted = evalOffered(replicaId);
                    }
                }
            } else {
                if (desired.timestamp > offeredTimestamps[replicaId]) {
                    offeredValues[replicaId] = desired.value;
                    offeredTimestamps[replicaId] = desired.timestamp;
                    accepted = evalOffered(replicaId);
                }
            }
        }
        return accepted != null;
    }

    // expected that instance's lock is being held
    private ValueTimestamp<V>[] evalOffered(int replicaId) {
        int[] qi = quorumIndexSet(offeredTimestamps);
        ValueTimestamp<V>[] accepted = null;
        if (qi != null) {
            accepted = new ValueTimestamp[qi.length];
            for (int i = 0; i < qi.length; i++) {
                if (commitedTimestamps[qi[i]] < offeredTimestamps[qi[i]]) {
                    commitedTimestamps[qi[i]] = offeredTimestamps[qi[i]];
                    commitedValues[qi[i]] = offeredValues[qi[i]];
                    accepted[i] = new ValueTimestamp<>( (V)commitedValues[qi[i]], commitedTimestamps[qi[i]]);
                }
            }
            // This is the cleanup hook which a node can use to ensure all values are fully replicated
            if (qi.length == offeredTimestamps.length) {
                long offeredTimestamp = offeredTimestamps[0];
                System.out.println("-- Cleanup offeredTimestamps on nodeId:" + replicaId + " --");
                for (int i = 0; i < qi.length; i++) {
                    offeredTimestamps[i] = -1;
                    offeredValues[i] = null;
                    if (commitedTimestamps[i] < offeredTimestamp) {
                        commitedTimestamps[i] = -1;
                        commitedValues[i] = null;
                    }
                }
            }
        }
        return accepted;
    }

    synchronized public ValueTimestamp get(long highwaterTimestamp) {
        int[] qi = quorumIndexSet(commitedTimestamps);
        if (qi != null && commitedTimestamps[qi[0]] >= highwaterTimestamp) {
            return new ValueTimestamp(commitedValues[qi[0]], commitedTimestamps[qi[0]]);
        }
        return null;
    }

    synchronized public Update<V>[] takeUpdates(int takerReplicaId) {
        int replication = offeredTimestamps.length;

        Update[] updates = new Update[replication];
        int offeredIndex = -1;
        int committedIndex = -1;
        long t = Long.MIN_VALUE;
        for (int nodeId = 0; nodeId < replication; nodeId++) {
            if (nodeId != takerReplicaId) {
                if (commitedTimestamps[nodeId] > -1 && commitedTimestamps[nodeId] > t) {
                    offeredIndex = -1;
                    t = commitedTimestamps[nodeId];
                    committedIndex = nodeId;
                }
                if (offeredTimestamps[nodeId] > -1 && offeredTimestamps[nodeId] > t) {
                    committedIndex = -1;
                    t = offeredTimestamps[nodeId];
                    offeredIndex = nodeId;
                }
            }
        }
        for (int nodeId = 0; nodeId < replication; nodeId++) {
            if (nodeId != takerReplicaId) {
                if (offeredIndex != -1) {
                    updates[nodeId] = new Update(nodeId, offeredValues[offeredIndex], offeredTimestamps[offeredIndex]);
                }
                if (committedIndex != -1) {
                    updates[nodeId] = new Update(nodeId, commitedValues[committedIndex], commitedTimestamps[committedIndex]);
                }
            }
        }
        return updates;
    }

    /**
     * Takes a list of timestamps and returns the set of indexs that comprise a quorum or null
     *
     * @param timestamps
     * @return
     */
    static public int[] quorumIndexSet(long[] timestamps) {
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
        sortLI(copyOfTimestamps, keys, 0, replication);

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

    public static void sortLI(long[] x, int[] keys, int off, int len) {
        int m;
        int v;
        if (len < 7) {
            for (m = off; m < len + off; ++m) {
                for (v = m; v > off && x[v - 1] > x[v]; --v) {
                    swapLO(x, keys, v, v - 1);
                }
            }

        } else {
            m = off + (len >> 1);
            int a;
            if (len > 7) {
                v = off;
                int n = off + len - 1;
                if (len > 40) {
                    a = len / 8;
                    v = med3L(x, off, off + a, off + 2 * a);
                    m = med3L(x, m - a, m, m + a);
                    n = med3L(x, n - 2 * a, n - a, n);
                }

                m = med3L(x, v, m, n);
            }

            double var13 = (double) x[m];
            a = off;
            int b = off;
            int c = off + len - 1;
            int d = c;

            while (true) {
                while (b > c || (double) x[b] > var13) {
                    for (; c >= b && (double) x[c] >= var13; --c) {
                        if ((double) x[c] == var13) {
                            swapLO(x, keys, c, d--);
                        }
                    }

                    if (b > c) {
                        int n1 = off + len;
                        int s = Math.min(a - off, b - a);
                        vecswapLO(x, keys, off, b - s, s);
                        s = Math.min(d - c, n1 - d - 1);
                        vecswapLO(x, keys, b, n1 - s, s);
                        if ((s = b - a) > 1) {
                            sortLI(x, keys, off, s);
                        }

                        if ((s = d - c) > 1) {
                            sortLI(x, keys, n1 - s, s);
                        }

                        return;
                    }

                    swapLO(x, keys, b++, c--);
                }

                if ((double) x[b] == var13) {
                    swapLO(x, keys, a++, b);
                }

                ++b;
            }
        }
    }

    private static void swapLO(long[] x, int[] keys, int a, int b) {
        long t = x[a];
        x[a] = x[b];
        x[b] = t;
        int l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;
    }

    private static void vecswapLO(long[] x, int[] keys, int a, int b, int n) {
        for (int i = 0; i < n; ++b) {
            swapLO(x, keys, a, b);
            ++i;
            ++a;
        }

    }

    private static int med3L(long[] x, int a, int b, int c) {
        return x[a] < x[b] ? (x[b] < x[c] ? b : (x[a] < x[c] ? c : a)) : (x[b] > x[c] ? b : (x[a] > x[c] ? c : a));
    }


    @Override
    public String toString() {
        return "offers:" + Arrays.toString(offeredTimestamps) + "=" + Arrays.toString(offeredValues)
            + " quorums:" + Arrays.toString(commitedTimestamps) + "=" + Arrays.toString(commitedValues);
    }
}
