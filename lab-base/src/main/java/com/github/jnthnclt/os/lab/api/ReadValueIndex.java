package com.github.jnthnclt.os.lab.api;

/**
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    String name();

    boolean get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception;

    /*
     from has to be an exact match. When caller knows from exactly they can avoid
     n log n cost to find start of range and instead leverage hash index
     */
    boolean pointRangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception;

    boolean rangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception;

    boolean rangesScan(Ranges ranges, ValueStream stream, boolean hydrateValues) throws Exception;

    /*
    The expectation is that the keys are in the same lex order as the value index.
     */
    boolean rowScan(ScanKeys keys, ValueStream stream, boolean hydrateValues) throws Exception;

    boolean rowScan(ValueStream stream, boolean hydrateValues) throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}