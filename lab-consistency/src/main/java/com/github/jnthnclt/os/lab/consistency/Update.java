package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class Update extends KT {
    public final int id;

    public Update(int id, long value, long timestamp) {
        super(value, timestamp);
        this.id = id;
    }
}
