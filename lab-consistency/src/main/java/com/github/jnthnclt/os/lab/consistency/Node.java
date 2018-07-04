package com.github.jnthnclt.os.lab.consistency;

import com.github.jnthnclt.os.lab.base.IndexUtil;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by colt on 11/23/17.
 */
public class Node {

    private final int replicaId;

    private final AtomicLong nodeTx = new AtomicLong();


    private final int replication;

    public static class Consistent {
        public final long[] offeredTimestamps;
        public final long[] offeredValues;

        public final long[] commitedTimestamps;
        public final long[] commitedValues;

        Consistent(int replication) {
            this.offeredTimestamps = new long[replication];
            Arrays.fill(this.offeredTimestamps, -1);
            this.offeredValues = new long[replication];
            Arrays.fill(this.offeredValues, -1);

            this.commitedTimestamps = new long[replication];
            Arrays.fill(this.commitedTimestamps, -1);
            this.commitedValues = new long[replication];
            Arrays.fill(this.commitedValues, -1);
        }

        @Override
        public String toString() {
            return "offers:" + Arrays.toString(offeredTimestamps) + "=" + Arrays.toString(offeredValues)
                + " quorums:" + Arrays.toString(commitedTimestamps) + "=" + Arrays.toString(commitedValues);
        }
    }

    private final ConcurrentSkipListMap<byte[], Consistent> map;

    public Node(int replicaId, int replication) {

        this.replicaId = replicaId;
        this.replication = replication;
        this.map = new ConcurrentSkipListMap<>((o1, o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length));
    }

    public String toString() {
        return replicaId + " " + map.toString();
    }

    public boolean set(byte[] key, KT expected, KT desired) {
        return set(replicaId, key, expected, desired);
    }

    private boolean set(int id, byte[] key, KT expected, KT desired) {

        Consistent consistent = map.computeIfAbsent(key, bytes -> new Consistent(replication));

        synchronized (nodeTx) {

            long[] offeredTimestamps = consistent.offeredTimestamps;
            long[] offeredValues = consistent.offeredValues;

            if (expected != null) {
                long[] commitedTimestamps = consistent.commitedTimestamps;
                long[] commitedValues = consistent.commitedValues;
                int[] qi = QuorumIndex.qi(commitedTimestamps);
                if (qi != null && commitedTimestamps[qi[0]] == expected.timestamp && commitedValues[qi[0]] == expected.value) {
                    if (desired.timestamp > offeredTimestamps[id]) {
                        offeredValues[id] = desired.value;
                        offeredTimestamps[id] = desired.timestamp;
                        return evalOffered(id, consistent);
                    }
                }
            } else {
                if (desired.timestamp > offeredTimestamps[id]) {
                    offeredValues[id] = desired.value;
                    offeredTimestamps[id] = desired.timestamp;
                    return evalOffered(id, consistent);
                }
            }
        }
        return false;
    }

    // expected that nodeTx lock is being held
    private static boolean evalOffered(int id, Consistent consistent) {
        long[] offeredTimestamps = consistent.offeredTimestamps;
        long[] offeredValues = consistent.offeredValues;

        long[] commitedTimestamps = consistent.commitedTimestamps;
        long[] commitedValues = consistent.commitedValues;

        int[] qi = QuorumIndex.qi(offeredTimestamps);
        if (qi != null) {
            for (int i = 0; i < qi.length; i++) {
                if (commitedTimestamps[qi[i]] < offeredTimestamps[qi[i]]) {
                    commitedTimestamps[qi[i]] = offeredTimestamps[qi[i]];
                    commitedValues[qi[i]] = offeredValues[qi[i]];
                }
            }
            // This is the cleanup hook which a node can use to ensure all values are fully replicated
            if (qi.length == offeredTimestamps.length) {
                System.out.println("-- Cleanup offeredTimestamps on nodeId:" + id + " --");
                for (int i = 0; i < qi.length; i++) {
                    offeredTimestamps[qi[i]] = -1;
                }
            }
        }
        return qi != null;
    }

    public KT get(byte[] key, long highwaterTimestamp, Node[] nodes, int depth) {

        Consistent consistent = map.computeIfAbsent(key, bytes -> new Consistent(replication));

        boolean repaired = false;
        for (int nodeId = 0; nodeId < nodes.length; nodeId++) {

            if (nodeId == replicaId) {
                KT got = get(consistent, highwaterTimestamp);
                if (got == null) {
                    if (depth == 0) {
                        int neighborNodeId = nodeId + 1;
                        if (neighborNodeId >= nodes.length) {
                            neighborNodeId = 0;
                        }
                        nodes[neighborNodeId].get(key, highwaterTimestamp, nodes, 1);
                    }
                }
            } else {
                nodes[nodeId].get(consistent, highwaterTimestamp);
            }

            if (nodeId != replicaId) {
                repaired = true;
                Update[] updates = nodes[nodeId].takeUpdates(replicaId, key);
                for (int i = 0; i < updates.length; i++) {
                    if (updates[i] != null) {
                        set(updates[i].id, key, null, updates[i]);
                    }
                }
            }
        }
        if (repaired) {
            return get(consistent, highwaterTimestamp);
        } else {
            return null;
        }
    }

    private KT get(Consistent consistent, long highwaterTimestamp) {
        synchronized (nodeTx) {
            long[] commitedTimestamps = consistent.commitedTimestamps;
            long[] commitedValues = consistent.commitedValues;

            int[] qi = QuorumIndex.qi(commitedTimestamps);
            if (qi != null && commitedTimestamps[qi[0]] >= highwaterTimestamp) {
                return new KT(commitedValues[qi[0]], commitedTimestamps[qi[0]]);
            }
        }
        return null;
    }


    public Update[] takeUpdates(int takerId, byte[] key) {
        Consistent consistent = map.computeIfAbsent(key, bytes -> new Consistent(replication));

        Update[] updates = new Update[replication];
        synchronized (nodeTx) {

            long[] commitedTimestamps = consistent.commitedTimestamps;
            long[] commitedValues = consistent.commitedValues;

            long[] offeredTimestamps = consistent.offeredTimestamps;
            long[] offeredValues = consistent.offeredValues;

            int offeredIndex = -1;
            int committedIndex = -1;
            long t = Long.MIN_VALUE;
            for (int nodeId = 0; nodeId < replication; nodeId++) {
                if (nodeId != takerId) {
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
                if (nodeId != takerId) {
                    if (offeredIndex != -1) {
                        updates[nodeId] = new Update(nodeId, offeredValues[offeredIndex], offeredTimestamps[offeredIndex]);
                    }
                    if (committedIndex != -1) {
                        updates[nodeId] = new Update(nodeId, commitedValues[committedIndex], commitedTimestamps[committedIndex]);
                    }
                }
            }
        }
        return updates;
    }
}
