package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.search.LABSearchFailFastOdometer.FailFastOdometerResult;
import com.github.jnthnclt.os.lab.core.search.LABSearchFailFastOdometer.FailFastdometer;
import com.github.jnthnclt.os.lab.core.search.LABSearchOdometer.Odometer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FailFastOdometerTest {

    @Test
    public void testSingleDigit() throws Exception {
        FailFastdometer fastdometer = new FailFastdometer(Arrays.asList("a", "b", "c", null), null);
        List<String> parts = Lists.newArrayList();


        List<List<String>> expected = Lists.newArrayList();
        expected.add(Arrays.asList("a"));
        expected.add(Arrays.asList("b"));
        expected.add(Arrays.asList("c"));
        expected.add(null);

        evaluate(fastdometer, parts, expected);
    }

    @Test
    public void testDoubleDigit() throws Exception {
        FailFastdometer fastdometer = new FailFastdometer(Arrays.asList("1", null, "3"), null);
        fastdometer = new FailFastdometer(Arrays.asList(null, "b", "c"), fastdometer);
        List<String> parts = Lists.newArrayList();


        List<List<String>> expected = Lists.newArrayList();
        expected.add(Arrays.asList(null, "1"));
        expected.add(null);
        expected.add(Arrays.asList(null, "3"));
        expected.add(Arrays.asList("b", "1"));
        expected.add(Arrays.asList("b", null));
        expected.add(Arrays.asList("b", "3"));
        expected.add(Arrays.asList("c", "1"));
        expected.add(Arrays.asList("c", null));
        expected.add(Arrays.asList("c", "3"));

        evaluate(fastdometer, parts, expected);
    }

    private void evaluate(FailFastdometer fastdometer, List<String> parts, List<List<String>> expected) {
        int i = 0;
        while (fastdometer.hasNext()) {
            parts.clear();
            FailFastOdometerResult<String, RoaringBitmap> r = fastdometer.next(parts, "all", null,
                (priorValue, priorResult, current) -> {
                    return "";
                });

            if (r == null) {
                Assert.assertNull(expected.get(i));
            } else {
                Assert.assertEquals(r.pattern.toArray(new String[0]), expected.get(i).toArray(new String[0]));
            }
            i++;
        }
    }


    @Test
    public void testSimpleFastCombinations() throws Exception {
        Map<String, RoaringBitmap> index = Maps.newHashMap();
        index.put("x", create(6, 7));
        index.put("y", create(1, 2, 3, 4, 5));
        index.put("1", create(1, 2, 3));
        index.put("3", create(5, 6, 7));
        index.put("4", create(1, 2, 3));
        index.put("a", create(5, 1, 2));
        index.put("b", create(1, 2));
        index.put("c", create(6));
        index.put("p", create(1, 2, 3, 4));
        index.put("q", create(5, 6, 7));


        FailFastdometer failFastdometer4 = new FailFastdometer(Arrays.asList(null, "p", "q"), null);
        FailFastdometer failFastdometer3 = new FailFastdometer(Arrays.asList("a", "b", null, "c"), failFastdometer4);
        FailFastdometer failFastdometer2 = new FailFastdometer(Arrays.asList("1", null, "3", "4"), failFastdometer3);
        FailFastdometer<String, RoaringBitmap> failFastdometer1 = new FailFastdometer<>(Arrays.asList("x", "y"), failFastdometer2);

        List<String> parts = Lists.newArrayList();
        RoaringBitmap all = create(1, 2, 3, 4, 5, 6, 7);

        List<String> results = Lists.newArrayList();

        while (failFastdometer1.hasNext()) {
            parts.clear();
            FailFastOdometerResult<String, RoaringBitmap> r = failFastdometer1.next(parts, "all", all,
                (priorValue, priorResult, current) -> {
                    RoaringBitmap got = index.get(current);
                    RoaringBitmap and = RoaringBitmap.and(priorResult, got);

                    //System.out.println(priorValue + " " + current + " " + got);

                    if (and.getCardinality() == 0) {
                        return null;
                    }
                    return and;
                });
            if (r != null) {
                String m = "Result:" + r.pattern + " " + r.edge;
                //System.out.println(m);
                results.add(m);
            }
        }


        Odometer odometer4 = new Odometer(false, Arrays.asList(null, "p", "q"), null);
        Odometer odometer3 = new Odometer(false, Arrays.asList("a", "b", null, "c"), odometer4);
        Odometer odometer2 = new Odometer(false, Arrays.asList("1", null, "3", "4"), odometer3);
        Odometer<String> odometer1 = new Odometer<>(true, Arrays.asList("x", "y"), odometer2);
        int i = 0;
        while (odometer1.hasNext()) {
            List<String> next = odometer1.next();
            RoaringBitmap answer = all.clone();
            for (String s : next) {
                if (s != null) {
                    RoaringBitmap got = index.get(s);
                    if (got != null) {
                        answer = RoaringBitmap.and(answer, got);
                    }
                }
            }
            if (answer.getCardinality() > 0) {
                String m = "Result:" + next + " " + answer;
                System.out.println(m + " vs " + results.get(i));
                Assert.assertEquals(m, results.get(i));
                i++;
            }
        }

    }


    @Test
    public void testCombinations() throws Exception {
        Map<String, RoaringBitmap> index = Maps.newHashMap();
        index.put("x", create(1, 2, 3, 4));
        index.put("y", create(1, 2, 3, 4));
        index.put("1", create(1, 2, 3));
        index.put("3", create(5, 6, 7));
        index.put("4", create(1, 2, 3));
        index.put("a", create(1, 2));
        index.put("b", create(1, 2));
        index.put("c", create(1, 2));

        FailFastdometer failFastdometer3 = new FailFastdometer(Arrays.asList("a", "b", "c", null), null);
        FailFastdometer failFastdometer2 = new FailFastdometer(Arrays.asList("1", null, "3", "4"), failFastdometer3);
        FailFastdometer<String, RoaringBitmap> failFastdometer1 = new FailFastdometer<>(Arrays.asList("x", "y"), failFastdometer2);

        List<String> parts = Lists.newArrayList();
        RoaringBitmap all = create(1, 2, 3, 4, 5);

        while (failFastdometer1.hasNext()) {
            parts.clear();
            FailFastOdometerResult<String, RoaringBitmap> r = failFastdometer1.next(parts, "all", all,
                (priorValue, priorResult, current) -> {
                    RoaringBitmap got = index.get(current);
                    RoaringBitmap and = RoaringBitmap.and(priorResult, got);
                    System.out.println(priorValue + "->" + current + " " + and);
                    if (and.getCardinality() == 0) {
                        return null;
                    }
                    return and;
                });
            if (r != null) {
                System.out.println("Result:" + r.pattern + " " + r.edge);
            }
        }
    }

    private static final RoaringBitmap create(int... value) {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(value);
        return bitmap;
    }
}
