package com.github.jnthnclt.os.lab.core.api;

/**
 * @author jonathan.colt
 */
public interface ReadValueIndex {

    String name();

    boolean get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception;

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