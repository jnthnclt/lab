package com.github.jnthnclt.os.lab.consistency;

import java.util.Arrays;
import org.testng.annotations.Test;
import org.testng.Assert;

/**
 * Created by colt on 11/23/17.
 */
public class QuorumIndexNGTest {

    @Test
    public void testQI() throws Exception {


        Assert.assertTrue(QuorumIndex.qi(new long[]{0, 0}) == null, "expected null");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{-1, -1, -1}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, -1, -1}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{-1, 0, -1}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{-1, -1, 0}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{-1, 0, 0}), new int[]{1, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, -1, 0}), new int[]{0, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, -1}), new int[]{0, 1}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 0}), new int[]{0, 1, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1}), new int[]{0, 1}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 0}), new int[]{0, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{1, 0, 0}), new int[]{1, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{1, 0, 1}), new int[]{0, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{1, 1, 0}), new int[]{0, 1}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 1}), new int[]{1, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{1, 1, 1}), new int[]{0, 1, 2}), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 2}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1, 1}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1, 2}), null), "");
        Assert.assertTrue(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 0, 1}), new int[]{0, 1, 2}), "");


//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 0, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 1, 0, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 1, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 0, 1 })) + "=1 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 1, 1 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 1, 1, 1 })) + "=2 ?");
    }
}
