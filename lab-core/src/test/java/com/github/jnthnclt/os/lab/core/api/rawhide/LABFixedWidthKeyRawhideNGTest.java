package com.github.jnthnclt.os.lab.core.api.rawhide;

import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import java.io.IOException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABFixedWidthKeyRawhideNGTest {

    @Test
    public void rawEntryTest() throws IOException {
        LABFixedWidthKeyRawhide rawhide = new LABFixedWidthKeyRawhide(8);
        BolBuffer rawEntry = rawhide.toRawEntry(UIO.longBytes(17), 1234, false, 687, UIO.longBytes(45), new BolBuffer());

        Assert.assertEquals(1234, rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry));
        Assert.assertEquals(687, rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry));

    }

    @Test
    public void mergeCompareTest() throws Exception {
        LABFixedWidthKeyRawhide rawhide = new LABFixedWidthKeyRawhide(8);
        BolBuffer a = new BolBuffer();
        BolBuffer b = new BolBuffer();
        set(rawhide, 1, 0, 0, a);
        set(rawhide, 1, 0, 0, b);
        assertMergeCompare(rawhide, a, b, 0);
        assertMergeCompare(rawhide, b, a, 0);

        set(rawhide, 1, 0, 0, a);
        set(rawhide, 2, 0, 0, b);
        assertMergeCompare(rawhide, a, b, -1);
        assertMergeCompare(rawhide, b, a, 1);

        set(rawhide, 1, 1, 0, a);
        set(rawhide, 1, 2, 0, b);
        assertMergeCompare(rawhide, a, b, 1);
        assertMergeCompare(rawhide, b, a, -1);

        set(rawhide, 1, 0, 1, a);
        set(rawhide, 1, 0, 2, b);

        assertMergeCompare(rawhide, a, b, 1);
        assertMergeCompare(rawhide, b, a, -1);

    }

    private void set(Rawhide rawhide, long key, long timestamp, long version, BolBuffer bolBuffer) throws Exception {
        rawhide.toRawEntry(UIO.longBytes(key), timestamp, false, version, UIO.longBytes(1), bolBuffer);
    }

    private void assertMergeCompare(Rawhide rawhide, BolBuffer a, BolBuffer b, int expected) throws Exception {
        Assert.assertEquals(
            rawhide.mergeCompare(
                FormatTransformer.NO_OP, FormatTransformer.NO_OP, a, new BolBuffer(),
                FormatTransformer.NO_OP, FormatTransformer.NO_OP, b, new BolBuffer()
            ), expected
        );
    }

    @Test
    public void bbCompareTest() {
        LABFixedWidthKeyRawhide rawhide = new LABFixedWidthKeyRawhide(8);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(1), 0, 8), 0);
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
    public void compareKeyTest() throws IOException {

        LABFixedWidthKeyRawhide rawhide = new LABFixedWidthKeyRawhide(8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer(),
            new BolBuffer(UIO.longBytes(1))), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(0).copy()), new BolBuffer(),
            new BolBuffer(UIO.longBytes(1))), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer(),
            new BolBuffer(UIO.longBytes(0))), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> {
            try {
                return rawhide.compareKey(FormatTransformer.NO_OP,
                    FormatTransformer.NO_OP,
                    new BolBuffer(toRawEntry(o1).copy()), new BolBuffer(),
                    new BolBuffer(UIO.longBytes(o2)));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });

        System.out.println("hmm:" + Arrays.toString(sort));

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    private BolBuffer toRawEntry(long key) throws IOException {
        return new LABFixedWidthKeyRawhide(8).toRawEntry(UIO.longBytes(key), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
    }

    @Test
    public void compareKey2Test() throws IOException, Exception {
        LABFixedWidthKeyRawhide rawhide = new LABFixedWidthKeyRawhide(8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer()), 0);

        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(0).copy()), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer()), -1);

        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(1).copy()), new BolBuffer(),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            new BolBuffer(toRawEntry(0).copy()), new BolBuffer()), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> {
            try {
                return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(toRawEntry(o1).copy()), new BolBuffer(),
                    FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(toRawEntry(o2).copy()), new BolBuffer());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

}
