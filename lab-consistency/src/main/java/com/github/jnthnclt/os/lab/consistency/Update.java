package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class Update<V> extends ValueTimestamp<V> {
    public final int id;

    public Update(int id, V value, long timestamp) {
        super(value, timestamp);
        this.id = id;
    }
}
