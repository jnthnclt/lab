package com.github.jnthnclt.os.lab.collections.lh;

import com.beust.jcommander.internal.Sets;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class ConcurrentLHashTest {

    @Test
    public void testStream() throws Exception {
        ConcurrentLHash<String> concurrentHash = new ConcurrentLHash<>(10, -1, -2,16);
        Set<String> expected = Sets.newHashSet();
        for (long k = 0; k < 1_000; k++) {
            concurrentHash.put(k, String.valueOf(k));
            expected.add(String.valueOf(k));
        }

        Set<String> actual = Sets.newHashSet();
        concurrentHash.stream((key, value) -> {
            actual.add(value);
            return true;
        });

        assertEquals(actual, expected);
    }

    @Test
    public void testRemove() throws Exception {
        ConcurrentLHash<String> hash = new ConcurrentLHash<>(10, -1, -2,16);
        int records = 1_000;
        for (long k = 0; k < records; k++) {
            hash.put(k, String.valueOf(k));
        }

        assertEquals(hash.size(), records);

        hash.stream((key, value) -> {
            hash.remove(key);
            return true;
        });

        assertEquals(hash.size(), 0);
        for (int k = 0; k < records; k++) {
            String got = hash.get(k);
            assertNull(got);
        }
    }
}
