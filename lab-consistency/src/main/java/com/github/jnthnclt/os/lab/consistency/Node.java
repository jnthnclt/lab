package com.github.jnthnclt.os.lab.consistency;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by colt on 11/23/17.
 */
public class Node {

    private final int id;

    private final AtomicLong nodeTx = new AtomicLong();

    private final long[] offeredTimestamps;
    private final long[] offeredValues;

    private final long[] commitedTimestamps;
    private final long[] commitedValues;

    private final int replication;

    public Node(int id, int replication) {

        this.id = id;
        this.offeredTimestamps = new long[replication];
        Arrays.fill(this.offeredTimestamps, -1);
        this.offeredValues = new long[replication];
        Arrays.fill(this.offeredValues, -1);

        this.commitedTimestamps = new long[replication];
        Arrays.fill(this.commitedTimestamps, -1);
        this.commitedValues = new long[replication];
        Arrays.fill(this.commitedValues, -1);
        this.replication = replication;

    }

    public String toString() {
        return id + " offers:" + Arrays.toString(offeredTimestamps) + "=" + Arrays.toString(offeredValues)
                + " quorums:" + Arrays.toString(commitedTimestamps) + "=" + Arrays.toString(commitedValues);
    }

    public boolean set(KT expected, KT desired) {
        return set(id, expected, desired);
    }

    private boolean set(int id, KT expected, KT desired) {
        synchronized (nodeTx) {
            if (expected != null) {
                int[] qi = QuorumIndex.qi(commitedTimestamps);
                if (qi != null && commitedTimestamps[qi[0]] == expected.timestamp && commitedValues[qi[0]] == expected.value) {
                    if (desired.timestamp > offeredTimestamps[id]) {
                        offeredValues[id] = desired.value;
                        offeredTimestamps[id] = desired.timestamp;
                        return evalOffered();
                    }
                }
            } else {
                if (desired.timestamp > offeredTimestamps[id]) {
                    offeredValues[id] = desired.value;
                    offeredTimestamps[id] = desired.timestamp;
                    return evalOffered();
                }
            }
        }
        return false;
    }

    // expected that nodeTx lock is being held
    private boolean evalOffered() {
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

    public KT get(long highwaterTimestamp, Node[] nodes, int depth) {


        boolean repaired = false;
        for (int nodeId = 0; nodeId < nodes.length; nodeId++) {

            if (nodeId == id) {
                KT got = get(highwaterTimestamp);
                if (got == null) {
                    if (depth == 0) {
                        int neighborNodeId = nodeId + 1;
                        if (neighborNodeId >= nodes.length) {
                            neighborNodeId = 0;
                        }
                        nodes[neighborNodeId].get(highwaterTimestamp, nodes, 1);
                    }
                }
            } else {
                nodes[nodeId].get(highwaterTimestamp);
            }

            if (nodeId != id) {
                repaired = true;
                Update[] updates = nodes[nodeId].takeUpdates(id);
                for (int i = 0; i < updates.length; i++) {
                    if (updates[i] != null) {
                        set(updates[i].id, null, updates[i]);
                    }
                }
            }
        }
        if (repaired) {
            return get(highwaterTimestamp);
        } else {
            return null;
        }
    }

    private KT get(long highwaterTimestamp) {
        synchronized (nodeTx) {
            int[] qi = QuorumIndex.qi(commitedTimestamps);
            if (qi != null && commitedTimestamps[qi[0]] >= highwaterTimestamp) {
                return new KT(commitedValues[qi[0]], commitedTimestamps[qi[0]]);
            }
        }
        return null;
    }


    public Update[] takeUpdates(int takerId) {
        Update[] updates = new Update[replication];
        synchronized (nodeTx) {

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
