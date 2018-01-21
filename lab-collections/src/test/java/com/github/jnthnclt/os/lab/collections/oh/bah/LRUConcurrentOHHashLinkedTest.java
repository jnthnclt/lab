package com.github.jnthnclt.os.lab.collections.oh.bah;

import com.beust.jcommander.internal.Sets;
import com.github.jnthnclt.os.lab.collections.oh.LRUConcurrentOHLinkedHash;
import com.github.jnthnclt.os.lab.collections.oh.OHEqualer;
import com.github.jnthnclt.os.lab.collections.oh.OHasher;
import java.util.Set;
import junit.framework.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class LRUConcurrentOHHashLinkedTest {

    @Test
    public void test() throws Exception {
        LRUConcurrentOHLinkedHash<String,String> lruCache = new LRUConcurrentOHLinkedHash<>(10, 100, 0.2f, true, 4, OHasher.SINGLETON, OHEqualer.SINGLETON);
        lruCache.start("lru", 10, (t) -> {
            Assert.fail();
            return false;
        });
        Set<String> expected = Sets.newHashSet();
        for (int i = 0; i < 1_000; i++) {
            String k = String.valueOf(i);
            lruCache.put(k, k);
            expected.add(k);
        }

        Set<String> actual = Sets.newHashSet();
        lruCache.stream((key, value) -> {
            actual.add(value);
            return true;
        });
        System.out.println(actual.size() + " vs " + expected.size());
        assertTrue(actual.size() < expected.size());

        lruCache.stop();
    }
}
