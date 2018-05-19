package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.search.LABSearchOdometer.Odometer;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OdometerTest {
    @Test
    public void testCombinations() throws Exception {

        Odometer odometer3 = new Odometer(false, Arrays.asList("x", "y"), null);
        Odometer odometer2 = new Odometer(false, Arrays.asList("1", null, "3"), odometer3);
        Odometer<String> odometer1 = new Odometer<>(true, Arrays.asList("a", "b", "c"), odometer2);

        Assert.assertEquals(odometer1.combinations(), 2*3*3);

        List<List<String>> expected = Lists.newArrayList();
        expected.add(Arrays.asList("a", "1", "x"));
        expected.add(Arrays.asList("a", "1", "y"));
        expected.add(Arrays.asList("a", null, "x"));
        expected.add(Arrays.asList("a", null, "y"));
        expected.add(Arrays.asList("a", "3", "x"));
        expected.add(Arrays.asList("a", "3", "y"));
        expected.add(Arrays.asList("b", "1", "x"));
        expected.add(Arrays.asList("b", "1", "y"));
        expected.add(Arrays.asList("b", null, "x"));
        expected.add(Arrays.asList("b", null, "y"));
        expected.add(Arrays.asList("b", "3", "x"));
        expected.add(Arrays.asList("b", "3", "y"));
        expected.add(Arrays.asList("c", "1", "x"));
        expected.add(Arrays.asList("c", "1", "y"));
        expected.add(Arrays.asList("c", null, "x"));
        expected.add(Arrays.asList("c", null, "y"));
        expected.add(Arrays.asList("c", "3", "x"));
        expected.add(Arrays.asList("c", "3", "y"));

        int i = 0;
        for (List<String> o : odometer1) {
            //System.out.println(o+" vs "+expected.get(i));
            Assert.assertTrue(Arrays.deepEquals(o.toArray(new String[0]), expected.get(i).toArray(new String[0])));
            i++;
        }

    }

}