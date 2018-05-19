package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABCSLMIndex;
import com.github.jnthnclt.os.lab.core.guts.LABIndex;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABAppendOnlyAllocator;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMap;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMemory;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABIndexableMemory;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexNGTest {

    @Test
    public void testCSLGet() throws Exception {
        LABIndex<BolBuffer, BolBuffer> map = new LABCSLMIndex(LABRawhide.SINGLETON, new StripingBolBufferLocks(1024));
        testLABIndex(map);

    }

    @Test
    public void testGet() throws Exception {
        LABIndex<BolBuffer, BolBuffer> map = new LABConcurrentSkipListMap(new LABStats(new AtomicLong()),
            new LABConcurrentSkipListMemory(
                LABRawhide.SINGLETON, new LABIndexableMemory( new LABAppendOnlyAllocator("test", 2))
            ),
            new StripingBolBufferLocks(1024)
        );

        testLABIndex(map);

    }

    private void testLABIndex(LABIndex<BolBuffer, BolBuffer> map) throws Exception {
        BolBuffer key = new BolBuffer(UIO.longBytes(8));
        BolBuffer value1 = new BolBuffer(UIO.longBytes(10));

        map.compute( new BolBuffer(), key, new BolBuffer(),
            (b, existing) -> {
                if (existing == null) {
                    return value1;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value1.copy())) {
                    return existing;
                } else {
                    return value1;
                }
            },
            (added, reused) -> {
            }
        );
        BolBuffer got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 10L);

        BolBuffer value2 = new BolBuffer(UIO.longBytes(21));
        map.compute( new BolBuffer(),
            key,
            new BolBuffer(), (b, existing) -> {
                if (existing == null) {
                    return value2;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value2.copy())) {
                    return existing;
                } else {
                    return value2;
                }
            },
            (added, reused) -> {
            }
        );
        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);

        BolBuffer value3 = new BolBuffer(UIO.longBytes(10));

        map.compute(
            new BolBuffer(),
            key,
            new BolBuffer(),
            ( b, existing) -> {
                if (existing == null) {
                    return value3;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value3.copy())) {
                    return existing;
                } else {
                    return value3;
                }
            },
            (added, reused) -> {
            }
        );

        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);
    }

}
