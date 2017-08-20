package org.colt.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public class MemoryRawEntryFormat extends RawEntryFormat {

    public static final String NAME = "memoryRawEntryFormat";
    public static final MemoryRawEntryFormat SINGLETON = new MemoryRawEntryFormat();

    private MemoryRawEntryFormat() {
        super(0, 0);
    }

}
