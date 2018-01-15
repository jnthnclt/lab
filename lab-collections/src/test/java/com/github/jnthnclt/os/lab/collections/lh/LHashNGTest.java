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
package com.github.jnthnclt.os.lab.collections.lh;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class LHashNGTest {

    @Test
    public void testPut() throws Exception {

        LHash<String> map = new LHash<>(new LHMapState<String>(10, -1, -2));
        internalTestPuts(map);
    }

    @Test
    public void testLinkedPut() throws Exception {

        LHash<String> map = new LHash<>(new LHMapState<String>(10, -1, -2));
        internalTestPuts(map);
    }

    private void internalTestPuts(LHash<String> map) throws Exception {
        int count = 64;
        for (byte i = 0; i < count; i++) {
            map.put(i, String.valueOf(i));
        }

        map.stream(new Semaphore(10, true), (long key, String value) -> {
            //System.out.println(Arrays.toString(key) + "->" + value);
            return true;
        });

        for (byte i = 0; i < count; i++) {
            Assert.assertEquals(map.get(i), String.valueOf(i));
        }
    }

    @Test
    public void testRemove() throws Exception {
        Random r = new Random();
        LHash<String> map = new LHash<>(new LHMapState<String>(10, -1, -2));
        internalTestRemoves(map, r, false);
    }

    @Test
    public void testLinkedRemove() throws Exception {
        Random r = new Random();
        LHash<String> map = new LHash<>(new LHMapState<String>(10, -1, -2));
        internalTestRemoves(map, r, true);
    }

    private void internalTestRemoves(LHash<String> map, Random r, boolean assertOrder) throws Exception {

        LinkedHashMap<Long, String> validation = new LinkedHashMap<>();

        int count = 7;

        // Add all
        for (long i = 0; i < count; i++) {
            map.remove(i);
            validation.remove(i);
        }

        for (long i = 0; i < count; i++) {
            map.put(i, String.valueOf(i));
            validation.put(i , String.valueOf(i));
        }

        if (assertOrder) {
            assertOrder("1 ", validation, map);
        }

        // Remove ~ half
        long[] retained = new long[count];
        Arrays.fill(retained,-1);
        long[] removed = new long[count];
        Arrays.fill(removed,-1);
        for (long i = 0; i < count; i++) {
            if (r.nextBoolean()) {
                //System.out.println("Removed:" + i);
                map.remove(i);
                validation.remove(i);
                removed[(int)i] = i;
            } else {
                retained[(int)i] = i;
            }
        }

        if (assertOrder) {
            assertOrder("2 ", validation, map);
        }

        for (long bs : retained) {
            if (bs >= 0) {
                Assert.assertEquals(map.get(bs), String.valueOf(bs));
            }
        }

        for (long bs : removed) {
            if (bs >= 0) {
                Assert.assertEquals(map.get(bs), null);
            }
        }

        // Add all back
        for (long i = 0; i < count; i++) {
            map.put(i, String.valueOf(i));
            validation.put(i, String.valueOf(i));
        }



        // Remove ~ half
        retained = new long[count];
        Arrays.fill(retained,-1);
        removed = new long[count];
        Arrays.fill(removed,-1);
        for (int i = 0; i < count; i++) {
            if (r.nextBoolean()) {
                //System.out.println("Removed:" + i);
                map.remove(i);
                validation.remove(i);
                removed[i] = i;
            } else {
                retained[i] = i;
            }
        }

        for (long bs : retained) {
            if (bs >=0) {
                Assert.assertEquals(map.get(bs), String.valueOf(bs));
            }
        }

        for (long bs : removed) {
            if (bs >=0 ) {
                Assert.assertEquals(map.get(bs), null);
            }
        }

        // Add all back
        for (int i = 0; i < count; i++) {
            map.put(i, String.valueOf(i));
            validation.put((long)i, String.valueOf(i));
        }

        // Remove all in reverse order
        for (byte i = (byte) count; i > -1; i--) {
            validation.remove(i);
            map.remove(i);
        }

        for (int i = 0; i < count; i++) {
            Assert.assertNull(map.get(i));
        }
    }

    private void assertOrder(String step, LinkedHashMap<Long, String> validation, LHash<String> map) throws Exception {
        Long[] expectedOrder = validation.keySet().toArray(new Long[0]);
        int[] i = new int[] { 0 };
        try {
            map.stream(new Semaphore(10, true), (long key, String value) -> {
                Assert.assertTrue(key == expectedOrder[i[0]], key + " vs " + expectedOrder[i[0]]);
                i[0]++;
                return true;
            });
            Assert.assertEquals(i[0], expectedOrder.length);
        } catch (Throwable x) {
            for (Long key : expectedOrder) {
                System.out.println(step + " Expected:" + key);
            }
            map.stream(new Semaphore(10, true), (long key, String value) -> {
                System.out.println(step + "Was:" + key);
                return true;
            });
            throw x;
        }
    }
}
