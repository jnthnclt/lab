package com.github.jnthnclt.os.lab.core.bitmaps;

import org.junit.Assert;
import org.testng.annotations.Test;

public class LABIndexKeyRangeTest {

    @Test
    public void testCompareTo() throws Exception {

        byte[] startInclusiveKey = { 1, 2, 3, 4 };
        byte[] stopExclusiveKey = { 2, 2, 3, 4 };

        LABIndexKeyRange a = new LABIndexKeyRange(
            startInclusiveKey,
            stopExclusiveKey);

        Assert.assertEquals(startInclusiveKey, a.getStartInclusiveKey());
        Assert.assertEquals(stopExclusiveKey, a.getStopExclusiveKey());

        Assert.assertFalse(a.contains(new byte[]{1, 2, 3, 3}));

        Assert.assertTrue(a.contains(startInclusiveKey));
        Assert.assertTrue(a.contains(new byte[]{1,3,3,3}));

        Assert.assertFalse(a.contains(stopExclusiveKey));

        Assert.assertFalse(a.contains(new byte[]{2, 2, 3, 5}));

    }


}