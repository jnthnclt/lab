package com.github.jnthnclt.os.lab.consistency;

import com.github.jnthnclt.os.lab.base.IndexUtil;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by colt on 11/23/17.
 */
public class Node<V> {

    private final int replicaId;
    private final int replication;
    private final ValuesEqual<V> valuesEqual;

    private final ConcurrentSkipListMap<byte[], ConsistentValue<V>> map;

    public Node(int replicaId, int replication, ValuesEqual<V> valuesEqual) {
        this.replicaId = replicaId;
        this.replication = replication;
        this.valuesEqual = valuesEqual;
        this.map = new ConcurrentSkipListMap<>((o1, o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length));
    }

    public boolean set(byte[] key, ValueTimestamp expected, ValueTimestamp desired, Transport transport) {
        return set(replicaId, key, expected, desired, transport);
    }

    private boolean set(int replicaId, byte[] key, ValueTimestamp expected, ValueTimestamp desired, Transport transport) {
        ConsistentValue<V> consistentValue = getConsistentValue(key);
        boolean set = consistentValue.set(replicaId, expected, desired, valuesEqual);
        if (set) {
            // TODO async, quorum, all
            talkAround(key, expected, desired, transport);
        }
        return set;
    }

    public ValueTimestamp<V> get(byte[] key, long highwaterTimestamp, Transport transport) {
        ConsistentValue<V> consistentValue = getConsistentValue(key);
        // TODO async, quorum, all
        askAround(key, transport);
        return consistentValue.get(highwaterTimestamp);
    }

    public Update<V>[] takeUpdates(int takerReplicaId, byte[] key) {
        ConsistentValue<V> consistentValue = getConsistentValue(key);
        return consistentValue.takeUpdates(takerReplicaId);
    }

    public String toString() {
        return replicaId + " " + map.toString();
    }

    private void talkAround(byte[] key,
        ValueTimestamp expected,
        ValueTimestamp desired,
        Transport transport) {

        int replicationCount = transport.count();
        for (int id = 0; id < replicationCount; id++) {
            if (id != replicaId) {
                transport.set(id, key, expected, desired);
            }
        }
    }

    private void askAround(byte[] key, Transport transport) {

        int replicationCount = transport.count();
        for (int id = 0; id < replicationCount; id++) {
            if (id != replicaId) {
                Update[] updates = transport.takeUpdates(id, replicaId, key);
                for (int i = 0; i < updates.length; i++) {
                    if (updates[i] != null) {
                        set(updates[i].id, key, null, updates[i], transport);
                    }
                }
            }
        }
    }


    private ConsistentValue<V> getConsistentValue(byte[] key) {
        return map.computeIfAbsent(key, bytes -> new ConsistentValue(replication));
    }
}
