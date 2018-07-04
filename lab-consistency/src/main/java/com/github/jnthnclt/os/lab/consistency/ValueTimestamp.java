package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class ValueTimestamp<V> {
    public final V value;
    public final long timestamp;

    public ValueTimestamp(V value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "value=" + value + " timestamp=" + timestamp;
    }
}
