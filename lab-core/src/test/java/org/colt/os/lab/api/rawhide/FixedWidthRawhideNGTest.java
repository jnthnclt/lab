package org.colt.os.lab.api.rawhide;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.colt.os.lab.api.FormatTransformer;
import org.colt.os.lab.guts.AppendOnlyFile;
import org.colt.os.lab.guts.ReadOnlyFile;
import org.colt.os.lab.io.AppendableHeap;
import org.colt.os.lab.io.BolBuffer;
import org.colt.os.lab.io.PointerReadableByteBufferFile;
import org.colt.os.lab.io.api.IAppendOnly;
import org.colt.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class FixedWidthRawhideNGTest {

    @Test
    public void rawEntryTest() throws IOException, Exception {
        Rawhide rawhide = new FixedWidthRawhide(8, 8);
        BolBuffer rawEntry = rawhide.toRawEntry(UIO.longBytes(17), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
        Assert.assertEquals(0, rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer()));
        Assert.assertEquals(0, rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer()));

        Assert.assertEquals(true, rawhide.isNewerThan(0, 0, 0, 0));

        Assert.assertEquals(false, rawhide.mightContain(-1, -1, 0, 0));
        Assert.assertEquals(true, rawhide.mightContain(1, 1, 0, 0));

        Assert.assertEquals(17, rawEntry.getLong(0));
        Assert.assertEquals(45, rawEntry.getLong(8));

        AppendableHeap appendableHeap = new AppendableHeap(1);
        rawhide.writeRawEntry(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP, appendableHeap);

        rawhide.streamRawEntry(0, FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(appendableHeap.getBytes()), new BolBuffer(),
            new BolBuffer(),
            (int index, BolBuffer key, long timestamp, boolean tombstoned, long version, BolBuffer payload) -> {
                Assert.assertEquals(17, key.getLong(0));
                Assert.assertEquals(45, payload.getLong(0));
                return true;
            }
        );

        File file = new File(Files.createTempDir(), "FixedWidthRawhideNGTest.rawEntryTest");

        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        IAppendOnly appender = appendOnlyFile.appender();
        rawhide.writeRawEntry(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP, appender);
        BolBuffer rawEntry2 = rawhide.toRawEntry(UIO.longBytes(33), 1234, false, 687, UIO.longBytes(99), new BolBuffer());
        rawhide.writeRawEntry(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry2, FormatTransformer.NO_OP, FormatTransformer.NO_OP, appender);
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
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(1), 0, 8), 0);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(10), 0, 8, UIO.longBytes(20), 0, 8), -10);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(0), 0, 8, UIO.longBytes(1), 0, 8), -1);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(0), 0, 8), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return rawhide.compareBB(UIO.longBytes(o1), 0, 8, UIO.longBytes(o2), 0, 8);
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKeyTest() throws Exception {
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer(),
            new BolBuffer(UIO.longBytes(1))), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(0)), new BolBuffer(),
            new BolBuffer(UIO.longBytes(1))), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer(),
            new BolBuffer(UIO.longBytes(0))), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort,
            (Long o1, Long o2) -> {
                try {
                    return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(o1)), new BolBuffer(),
                        new BolBuffer(UIO.longBytes(o2)));
                } catch (Exception x) {
                    throw new RuntimeException(x);
                }
            });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKey2Test() throws Exception {
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer()), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(0)), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer()), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(1)), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(0)), new BolBuffer()), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (Long o1, Long o2) -> {
            try {
                return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(o1)), new BolBuffer(),
                    FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(UIO.longBytes(o2)), new BolBuffer());
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

}
