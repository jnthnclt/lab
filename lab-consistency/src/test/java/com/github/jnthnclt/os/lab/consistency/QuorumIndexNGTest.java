package com.github.jnthnclt.os.lab.consistency;

import org.testng.annotations.Test;
import sun.jvm.hotspot.utilities.Assert;

import java.util.Arrays;

/**
 * Created by colt on 11/23/17.
 */
public class QuorumIndexNGTest {

    @Test
    public void testQI() throws Exception {


        Assert.that(QuorumIndex.qi(new long[]{0, 0}) == null, "expected null");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{-1, -1, -1}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, -1, -1}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{-1, 0, -1}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{-1, -1, 0}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{-1, 0, 0}), new int[]{1, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, -1, 0}), new int[]{0, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, -1}), new int[]{0, 1}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 0}), new int[]{0, 1, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1}), new int[]{0, 1}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 0}), new int[]{0, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{1, 0, 0}), new int[]{1, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{1, 0, 1}), new int[]{0, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{1, 1, 0}), new int[]{0, 1}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 1}), new int[]{1, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{1, 1, 1}), new int[]{0, 1, 2}), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 1, 2}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1, 1}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 1, 2}), null), "");
        Assert.that(Arrays.equals(QuorumIndex.qi(new long[]{0, 0, 0, 1}), new int[]{0, 1, 2}), "");


//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 0, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 1, 0, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 1, 0 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 0, 1 })) + "=1 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 0, 1, 1 })) + "=2 ?");
//        System.out.println(Arrays.toString(QuorumIndex.qi(new long[] { 1, 1, 1 })) + "=2 ?");
    }
}
