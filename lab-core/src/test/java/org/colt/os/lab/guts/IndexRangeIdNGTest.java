package org.colt.os.lab.guts;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class IndexRangeIdNGTest {

    @Test
    public void testIntersects() {
        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(6, 9, 0)));
        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(5, 10, 0)));
        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(0, 6, 0)));
        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(9, 20, 0)));

        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(0, 5, 0)));
        Assert.assertTrue(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(10, 20, 0)));

        Assert.assertFalse(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(0, 4, 0)));
        Assert.assertFalse(new IndexRangeId(5, 10, 0).intersects(new IndexRangeId(11, 20, 0)));

    }

}
