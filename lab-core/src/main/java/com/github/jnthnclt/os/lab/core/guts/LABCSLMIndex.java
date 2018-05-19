/*
 * Copyright 2016 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABCostChangeInBytes;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author jonathan.colt
 */
public class LABCSLMIndex implements LABIndex<BolBuffer, BolBuffer> {

    private final ConcurrentSkipListMap<byte[], byte[]> map;
    private final StripingBolBufferLocks bolBufferLocks;
    private final int seed;

    public LABCSLMIndex(Rawhide rawhide, StripingBolBufferLocks bolBufferLocks) {
        this.map = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());
        this.bolBufferLocks = bolBufferLocks;
        this.seed = System.identityHashCode(this);
    }

    @Override
    public void compute(
        BolBuffer rawEntry,
        BolBuffer keyBytes,
        BolBuffer valueBuffer,
        Compute<BolBuffer, BolBuffer> remappingFunction,
        LABCostChangeInBytes changeInBytes) {

        synchronized (bolBufferLocks.lock(keyBytes, seed)) {
            map.compute(keyBytes.copy(), // Grrr
                (k, v) -> {
                    long cost = 0;
                    try {
                        BolBuffer apply;
                        if (v != null) {
                            valueBuffer.force(v, 0, v.length);
                            apply = remappingFunction.apply(rawEntry, valueBuffer);
                            cost = ((apply == null || apply.length == -1) ? 0 : apply.length) - v.length;
                        } else {
                            apply = remappingFunction.apply(rawEntry, null);
                            if (apply != null && apply.length > -1) {
                                cost = k.length + apply.length;
                            }
                        }
                        if (apply != null && v == apply.bytes) {
                            return v;
                        } else {
                            return apply == null ? null : apply.copy();
                        }
                    } finally {
                        changeInBytes.cost(cost, 0);
                    }
                });
        }
    }

    @Override
    public BolBuffer get(BolBuffer key, BolBuffer valueBuffer) {
        byte[] got = map.get(key.copy()); // Grrr
        if (got == null) {
            return null;
        }
        valueBuffer.set(got);
        return valueBuffer;
    }

    @Override
    public boolean contains(byte[] from, byte[] to) {
        return !subMap(from, to).isEmpty();
    }

    private Map<byte[], byte[]> subMap(byte[] from, byte[] to) {
        if (from != null && to != null) {
            return map.subMap(from, to);
        } else if (from != null) {
            return map.tailMap(from);
        } else if (to != null) {
            return map.headMap(to);
        } else {
            return map;
        }
    }

    @Override
    public Scanner scanner(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) {
        Iterator<Map.Entry<byte[], byte[]>> iterator = subMap(from, to)
            .entrySet()
            .iterator();
        return new Scanner() {
            @Override
            public BolBuffer next(BolBuffer rawEntry, BolBuffer nextHint) throws Exception {
                if (iterator.hasNext()) {
                    Map.Entry<byte[], byte[]> next = iterator.next();
                    byte[] value = next.getValue();
                    entryBuffer.force(value, 0, value.length);
                    return entryBuffer;
                }
                return null;
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public byte[] firstKey() {
        return map.firstKey();
    }

    @Override
    public byte[] lastKey() {
        return map.lastKey();
    }

    @Override
    public int poweredUpTo() {
        return -1;
    }

}
