package com.github.jnthnclt.os.lab.core.api;

/**
 *
 * @author jonathan.colt
 */
public class RawEntryFormat {

    final long keyFormat;
    final long valueFormat;

    public RawEntryFormat(long keyFormat, long valueFormat) {
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
    }

}
