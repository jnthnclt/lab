package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class KT {
    public final long value;
    public final long timestamp;

    public KT(long value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "value=" + value + " timestamp=" + timestamp;
    }
}
