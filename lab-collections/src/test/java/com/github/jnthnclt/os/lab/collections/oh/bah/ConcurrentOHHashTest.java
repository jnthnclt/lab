package com.github.jnthnclt.os.lab.collections.oh.bah;

import com.beust.jcommander.internal.Sets;
import com.github.jnthnclt.os.lab.collections.oh.ConcurrentOHash;
import com.github.jnthnclt.os.lab.collections.oh.OHEqualer;
import com.github.jnthnclt.os.lab.collections.oh.OHasher;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class ConcurrentOHHashTest {

    @Test
    public void testStream() throws Exception {
        ConcurrentOHash<String, String> concurrentOHHash = new ConcurrentOHash<>(
            10, true, 4, OHasher.SINGLETON, OHEqualer.SINGLETON);
        Set<String> expected = Sets.newHashSet();
        for (int i = 0; i < 1_000; i++) {
            String k = String.valueOf(i);
            concurrentOHHash.put(k, k);
            expected.add(k);
        }

        Set<String> actual = Sets.newHashSet();
        concurrentOHHash.stream((key, value) -> {
            actual.add(value);
            return true;
        });

        assertEquals(actual, expected);
    }

    @Test
    public void testRemove() throws Exception {
        ConcurrentOHash<String, String> concurrentOHHash = new ConcurrentOHash<>(
            10, true, 4, OHasher.SINGLETON, OHEqualer.SINGLETON);
        int records = 1_000;
        for (int i = 0; i < records; i++) {
            String k = String.valueOf(i);
            concurrentOHHash.put(k, k);
        }

        assertEquals(concurrentOHHash.size(), records);

        concurrentOHHash.stream((key, value) -> {
            concurrentOHHash.remove(key);
            return true;
        });

        assertEquals(concurrentOHHash.size(), 0);
        for (int i = 0; i < records; i++) {
            String k = String.valueOf(i);
            String got = concurrentOHHash.get(k);
            assertNull(got);
        }
    }
}
