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
package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.core.guts.AppendOnlyFile;
import com.github.jnthnclt.os.lab.core.guts.ReadOnlyFile;
import com.github.jnthnclt.os.lab.core.io.AppendableHeap;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABKeyValueRawhideNGTest {

    @Test
    public void rawEntryTest() throws IOException, Exception {

        Rawhide rawhide = LABKeyValueRawhide.SINGLETON;
        Assert.assertEquals(0, rawhide.timestamp( new BolBuffer()));
        Assert.assertEquals(0, rawhide.version( new BolBuffer()));

        Assert.assertEquals(true, rawhide.isNewerThan(0, 0, 0, 0));

        Assert.assertEquals(false, rawhide.mightContain(-1, -1, 0, 0));
        Assert.assertEquals(true, rawhide.mightContain(1, 1, 0, 0));

        BolBuffer rawEntry = rawhide.toRawEntry(UIO.longBytes(17), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        System.out.println(Arrays.toString(rawEntry.copy()));

        Assert.assertEquals(17, rawEntry.getLong(4));
        Assert.assertEquals(45, rawEntry.getLong(16));

        AppendableHeap appendableHeap = new AppendableHeap(1);
        rawhide.writeRawEntry(rawEntry,  appendableHeap);

        byte[] bytes = appendableHeap.getBytes();
        rawhide.streamRawEntry(0, new BolBuffer(bytes, 4, bytes.length - 4), new BolBuffer(), new BolBuffer(),
            (int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
                System.out.println(Arrays.toString(key.copy()));
                Assert.assertEquals(17, key.getLong(0));
                System.out.println(Arrays.toString(payload.copy()));
                Assert.assertEquals(45, payload.getLong(0));
                return true;
            }
        );

        File file = new File(Files.createTempDir(), "KeyValueRawhideNGTest.rawEntryTest");

        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        IAppendOnly appender = appendOnlyFile.appender();
        rawhide.writeRawEntry( rawEntry, appender);
        BolBuffer rawEntry2 = rawhide.toRawEntry(UIO.longBytes(33), 1234, false, 687, UIO.longBytes(99), new BolBuffer());
        rawhide.writeRawEntry( rawEntry2,  appender);
        appender.flush(true);
        appender.close();

        System.out.println(file.length());

        ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
        PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(-1);

        BolBuffer readRawEntry = new BolBuffer();
        int offset = rawhide.rawEntryToBuffer(pointerReadable, 0, readRawEntry);

        BolBuffer readRawEntry2 = new BolBuffer();
        rawhide.rawEntryToBuffer(pointerReadable, offset, readRawEntry2);

        Assert.assertEquals(rawEntry.copy(), readRawEntry.copy());
        Assert.assertEquals(rawEntry2.copy(), readRawEntry2.copy());

    }

    @Test
    public void bbCompareTest() {
        Rawhide rawhide = LABKeyValueRawhide.SINGLETON;
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(1), 0, 8), 0);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(10), 0, 8, UIO.longBytes(20), 0, 8), -10);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(0), 0, 8, UIO.longBytes(1), 0, 8), -1);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(0), 0, 8), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> rawhide.compareBB(UIO.longBytes(o1), 0, 8, UIO.longBytes(o2), 0, 8));

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKeyTest() throws Exception {
        Rawhide rawhide = LABKeyValueRawhide.SINGLETON;
        BolBuffer a = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        BolBuffer b = new BolBuffer(UIO.longBytes(1));
        Assert.assertEquals(rawhide.compareKey( a, new BolBuffer(), b), 0);

        a = rawhide.toRawEntry(UIO.longBytes(0), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        b = new BolBuffer(UIO.longBytes(1));
        Assert.assertEquals(rawhide.compareKey( a, new BolBuffer(), b), -1);

        a = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        b = new BolBuffer(UIO.longBytes(0));
        Assert.assertEquals(rawhide.compareKey(a, new BolBuffer(), b), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> {
            try {
                BolBuffer a1 = rawhide.toRawEntry(UIO.longBytes(o1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
                BolBuffer b1 = new BolBuffer(UIO.longBytes(o2));
                return rawhide.compareKey( a1, new BolBuffer(), b1);
            } catch (Exception ex) {
                Assert.fail();
                return 0;
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKey2Test() throws Exception {
        Rawhide rawhide = LABKeyValueRawhide.SINGLETON;
        BolBuffer a = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        BolBuffer b = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());

        Assert.assertEquals(rawhide.compareKey( a, new BolBuffer(),
             b, new BolBuffer()), 0);

        a = rawhide.toRawEntry(UIO.longBytes(0), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        b = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        Assert.assertEquals(rawhide.compareKey( a, new BolBuffer(),
             b, new BolBuffer()), -1);

        a = rawhide.toRawEntry(UIO.longBytes(1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        b = rawhide.toRawEntry(UIO.longBytes(0), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        Assert.assertEquals(rawhide.compareKey( a, new BolBuffer(),
             b, new BolBuffer()), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> {
            try {
                BolBuffer a1 = rawhide.toRawEntry(UIO.longBytes(o1), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
                BolBuffer b1 = rawhide.toRawEntry(UIO.longBytes(o2), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
                return rawhide.compareKey( a1, new BolBuffer(), b1, new BolBuffer());
            } catch (Exception ex) {
                Assert.fail();
                return 0;
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {

            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

}
