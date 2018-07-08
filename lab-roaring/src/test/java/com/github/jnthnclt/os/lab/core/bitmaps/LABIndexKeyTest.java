package com.github.jnthnclt.os.lab.core.bitmaps;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.testng.annotations.Test;

public class LABIndexKeyTest {
    @Test
    public void testCompareTo() throws Exception {

        Assert.assertEquals("foo",
            new LABIndexKey("foo".getBytes(StandardCharsets.UTF_8)).toString());

        LABIndexKey a = new LABIndexKey(new byte[] { 1, 2, 3, 4 });
        LABIndexKey b = new LABIndexKey(new byte[] { 1, 2, 3, 4 });

        Assert.assertEquals(0, a.compareTo(b));
        Assert.assertEquals(a, b);

        a = new LABIndexKey(new byte[] { 1, 2, 3, 4 });
        b = new LABIndexKey(new byte[] { 4, 3, 2, 1 });
        Assert.assertTrue(a.compareTo(b) < 0);
        Assert.assertTrue(b.compareTo(a) > 0);

        Assert.assertFalse(a.equals(b));
        Assert.assertFalse(a.equals(null));
        Assert.assertFalse(a.equals("foo"));

    }

}