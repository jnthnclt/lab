package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.core.guts.LABMap;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.google.common.collect.Maps;
import java.util.Map;

public class LABConcurrentHashMap implements LABMap<BolBuffer, BolBuffer> {

    final Map<BolBuffer, BolBuffer> memory = Maps.newConcurrentMap();

    @Override
    public void compute(BolBuffer entry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        Compute<BolBuffer, BolBuffer> computeFunction,
        LABCostChangeInBytes changeInBytes) throws Exception {



    }

    @Override
    public BolBuffer get(BolBuffer key, BolBuffer valueBuffer) throws Exception {
        return null;
    }

    @Override
    public boolean contains(byte[] from, byte[] to) throws Exception {
        return false;
    }

    @Override
    public Scanner scanner(boolean pointFrom,
        byte[] from,
        byte[] to,
        BolBuffer entryBuffer,
        BolBuffer entryKeyBuffer) throws Exception {

        return null;
    }

    @Override
    public void clear() throws Exception {
        memory.clear();
    }

    @Override
    public boolean isEmpty() throws Exception {
        return memory.isEmpty();
    }

    @Override
    public byte[] firstKey() throws Exception {
        return new byte[0];
    }

    @Override
    public byte[] lastKey() throws Exception {
        return new byte[0];
    }

    @Override
    public int poweredUpTo() {
        return 0;
    }
}
