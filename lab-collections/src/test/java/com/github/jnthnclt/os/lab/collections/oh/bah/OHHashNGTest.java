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
package com.github.jnthnclt.os.lab.collections.oh.bah;

import com.github.jnthnclt.os.lab.collections.oh.OHEqualer;
import com.github.jnthnclt.os.lab.collections.oh.OHLinkedMapState;
import com.github.jnthnclt.os.lab.collections.oh.OHMapState;
import com.github.jnthnclt.os.lab.collections.oh.OHash;
import com.github.jnthnclt.os.lab.collections.oh.OHasher;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class OHHashNGTest {

    @Test
    public void testPut() throws Exception {

        OHash<String, String> map = new OHash<>(new OHMapState<String, String>(10, true, ""), OHasher.SINGLETON, OHEqualer.SINGLETON);
        internalTestPuts(map);
    }

    @Test
    public void testLinkedPut() throws Exception {

        OHash<String, String> map = new OHash<>(new OHLinkedMapState<String, String>(10, true, ""), OHasher.SINGLETON, OHEqualer.SINGLETON);
        internalTestPuts(map);
    }

    private void internalTestPuts(OHash<String, String> map) throws Exception {
        int count = 64;
        for (byte i = 0; i < count; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
        }

        map.stream(new Semaphore(10, true), (key, value) -> {
            //System.out.println(Arrays.toString(key) + "->" + value);
            return true;
        });

        for (byte i = 0; i < count; i++) {
            Assert.assertEquals(map.get(String.valueOf(i)), String.valueOf(i));
        }
    }

    @Test
    public void testRemove() throws Exception {
        Random r = new Random();
        OHash<String, String> map = new OHash<>(new OHMapState<String, String>(10, true, ""), OHasher.SINGLETON, OHEqualer.SINGLETON);
        internalTestRemoves(map, r, false);
    }

    @Test
    public void testLinkedRemove() throws Exception {
        Random r = new Random();
        OHash<String, String> map = new OHash<>(new OHLinkedMapState<String, String>(10, true, ""), OHasher.SINGLETON, OHEqualer.SINGLETON);
        internalTestRemoves(map, r, true);
    }

    private void internalTestRemoves(OHash<String, String> map, Random r, boolean assertOrder) throws Exception {

        LinkedHashMap<String, String> validation = new LinkedHashMap<>();

        int count = 7;

        // Add all
        for (byte i = 0; i < count; i++) {
            map.remove(String.valueOf(i));
            validation.remove(String.valueOf(i));
        }

        for (byte i = 0; i < count; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            validation.put(String.valueOf(i), String.valueOf(i));
        }

        if (assertOrder) {
            assertOrder("1 ", validation, map);
        }

        // Remove ~ half
        String[] retained = new String[count];
        String[] removed = new String[count];
        for (byte i = 0; i < count; i++) {
            String key = String.valueOf(i);
            if (r.nextBoolean()) {
                //System.out.println("Removed:" + i);
                map.remove(key);
                validation.remove(String.valueOf(i));
                removed[i] = key;
            } else {
                retained[i] = key;
            }
        }

        if (assertOrder) {
            assertOrder("2 ", validation, map);
        }

        for (String bs : retained) {
            if (bs != null) {
                Assert.assertEquals(map.get(bs), bs);
            }
        }

        for (String bs : removed) {
            if (bs != null) {
                Assert.assertEquals(map.get(bs), null);
            }
        }

        // Add all back
        for (byte i = 0; i < count; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            validation.put(String.valueOf(i), String.valueOf(i));
        }

        if (assertOrder) {
            assertOrder("3 ", validation, map);
        }

        // Remove ~ half
        retained = new String[count];
        removed = new String[count];
        for (byte i = 0; i < count; i++) {
            String key = String.valueOf(i);
            if (r.nextBoolean()) {
                //System.out.println("Removed:" + i);
                map.remove(key);
                validation.remove(String.valueOf(i));
                removed[i] = key;
            } else {
                retained[i] = key;
            }
        }

        if (assertOrder) {
            assertOrder("4 ", validation, map);
        }

        for (String bs : retained) {
            if (bs != null) {
                Assert.assertEquals(map.get(bs), bs);
            }
        }

        for (String bs : removed) {
            if (bs != null) {
                Assert.assertEquals(map.get(bs), null);
            }
        }

        // Add all back
        for (byte i = 0; i < count; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
            validation.put(String.valueOf(i), String.valueOf(i));
        }

        if (assertOrder) {
            assertOrder("5 ", validation, map);
        }

        // Remove all in reverse order
        for (byte i = (byte) count; i > -1; i--) {
            validation.remove(String.valueOf(i));
            map.remove(String.valueOf(i));
        }

        if (assertOrder) {
            assertOrder("6 ", validation, map);
        }

    }

    private void assertOrder(String step, LinkedHashMap<String, String> validation, OHash<String, String> map) throws Exception {
        String[] expectedOrder = validation.keySet().toArray(new String[0]);
        int[] i = new int[] { 0 };
        try {
            map.stream(new Semaphore(10, true), (key, value) -> {
                Assert.assertTrue(key.equals(expectedOrder[i[0]]), key + " vs " + expectedOrder[i[0]]);
                i[0]++;
                return true;
            });
            Assert.assertEquals(i[0], expectedOrder.length);
        } catch (Throwable x) {
            for (String k : expectedOrder) {
                System.out.println(step + " Expected:" + k);
            }
            map.stream(new Semaphore(10, true), (key, value) -> {
                System.out.println(step + "Was:" + key);
                return true;
            });
            throw x;
        }

    }


}
