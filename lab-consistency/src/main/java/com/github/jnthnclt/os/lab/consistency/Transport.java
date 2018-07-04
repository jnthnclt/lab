package com.github.jnthnclt.os.lab.consistency;

public class Transport<V> {

    private final Node<V>[] nodes;

    public Transport(Node<V>[] nodes) {
        this.nodes = nodes;
    }

    public int count() {
        return nodes.length;
    }

    public Update[] takeUpdates(int nodeId, int replicaId, byte[] key) {
        return nodes[nodeId].takeUpdates(replicaId, key);
    }

    public boolean set(int nodeId,byte[] key, ValueTimestamp expected, ValueTimestamp desired) {
        return nodes[nodeId].set(key, expected, desired, this);
    }
}
