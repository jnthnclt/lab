package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.search.LABSearch.CachedFieldValue;
import com.google.common.io.Files;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

public class LABSearchIndexTest {

    @Test
    public void helloSearchTest() throws Exception {

        File root = Files.createTempDir();
        System.out.println(root.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats();
        LABIndexProvider labIndexProvider = new LABIndexProvider(globalHeapCostInBytes, stats);
        LABSearchIndex index = new LABSearchIndex(labIndexProvider, root);

        LABSearchIndexUpdates updates = new LABSearchIndexUpdates();

        int storedOrdinal = index.fieldOrdinal("stored");
        int titleOrdinal = index.fieldOrdinal("title");
        int bodyOrdinal = index.fieldOrdinal("body");
        int priceOrdinal = index.fieldOrdinal("price");

        long timestamp = System.currentTimeMillis();


        updates.store(0, storedOrdinal, timestamp, "{userid=1,searchId=5}".getBytes());
        updates.updateStrings(0, titleOrdinal, timestamp, new String[] { "a", "wrinkle", "in", "time" });
        updates.updateStrings(0, bodyOrdinal, timestamp, new String[] { "quick", "brown", "fox" });
        updates.updateStrings(0, priceOrdinal, timestamp, new String[] { "000799" });

        updates.store(1, storedOrdinal, timestamp, "{userid=1,searchId=7}".getBytes());
        updates.updateStrings(1, titleOrdinal, timestamp, new String[] { "a", "crease", "in", "time" });
        updates.updateStrings(1, bodyOrdinal, timestamp, new String[] { "slow", "red", "fox" });
        updates.updateStrings(1, priceOrdinal, timestamp, new String[] { "000699" });

        updates.store(2, storedOrdinal, timestamp, "{userid=1,searchId=9}".getBytes());
        updates.updateStrings(2, titleOrdinal, timestamp, new String[] { "a", "wrinkle", "in", "cloth" });
        updates.updateStrings(2, bodyOrdinal, timestamp, new String[] { "quick", "red", "fox" });
        updates.updateStrings(2, priceOrdinal, timestamp, new String[] { "000599" });

        updates.store(3, storedOrdinal, timestamp, "{userid=1,searchId=19}".getBytes());
        updates.updateStrings(3, titleOrdinal, timestamp, new String[] { "a", "wrinkle", "in", "cloth" });
        updates.updateStrings(3, bodyOrdinal, timestamp, new String[] { "quick", "red", "fox" });
        updates.updateStrings(3, priceOrdinal, timestamp, new String[] { "001599" });

        index.update(updates, false);
        index.flush();
        System.out.println(index.summary());
        index.compact();


        LABSearchCombinator<CachedFieldValue> combinator = LABSearchOdometer.buildOdometer(index,
            Arrays.asList("title", "body"),
            Arrays.asList(Arrays.asList("*"), Arrays.asList("brown", "fox", null)),
            true);

        System.out.println(combinator.combinations());
        while (combinator.hasNext()) {
            List<CachedFieldValue> next = combinator.next();
            RoaringBitmap answer = LABSearch.andFieldValues(index, next, null, null);
            System.out.println(answer.getCardinality() + " had " + next);
            index.storedValues(answer, storedOrdinal, (index1, value) -> {
                System.out.println(index1 + "->" + new String(value.copy()));
                return true;
            });
        }
    }

}