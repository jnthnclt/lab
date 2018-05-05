package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABFixedWidthKeyFixedWidthValueRawhide;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class LABConcurrentSkipListMapNGTest {

    @Test
    public void batTest() throws Exception {

        LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", 2);
        LABIndexableMemory labIndexableMemory = new LABIndexableMemory(allocator);
        LABFixedWidthKeyFixedWidthValueRawhide rawhide = new LABFixedWidthKeyFixedWidthValueRawhide(8, 8);

        LABConcurrentSkipListMap map = new LABConcurrentSkipListMap(new LABStats(), new LABConcurrentSkipListMemory(rawhide, labIndexableMemory),
            new StripingBolBufferLocks(1024));

        for (int i = 0; i < 100; i++) {

            BolBuffer key = new BolBuffer(UIO.longBytes(i));
            BolBuffer value = new BolBuffer(UIO.longBytes(i));
            map.compute(new BolBuffer(), key, value,
                (b, existing) -> value,
                (added, reused) -> {
                });
        }
        //System.out.println("Count:" + map.size());
        //System.out.println("first:" + UIO.bytesLong(map.firstKey()));
        //System.out.println("last:" + UIO.bytesLong(map.lastKey()));

        Scanner scanner = map.scanner(null, null, new BolBuffer(), new BolBuffer());
        while (scanner.next((rawEntry) -> {
            //System.out.println("Keys:" + UIO.bytesLong(rawEntry.copy()));
            return true;
        }, null) == Next.more) {
        }

    }

}
