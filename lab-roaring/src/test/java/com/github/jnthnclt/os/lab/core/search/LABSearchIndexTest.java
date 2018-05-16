package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.search.LABSearchIndex.CachedFieldValue;
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


        updates.updateStrings(0, storedOrdinal, null, "{userid=1,searchId=5}".getBytes());
        updates.updateStrings(0, titleOrdinal, new String[] { "a", "wrinkle", "in", "time" }, null);
        updates.updateStrings(0, bodyOrdinal, new String[] { "quick", "brown", "fox" }, null);
        updates.updateStrings(0, priceOrdinal, new String[] { "000799" }, null);

        updates.updateStrings(1, storedOrdinal, null, "{userid=1,searchId=7}".getBytes());
        updates.updateStrings(1, titleOrdinal, new String[] { "a", "crease", "in", "time" }, null);
        updates.updateStrings(1, bodyOrdinal, new String[] { "slow", "red", "fox" }, null);
        updates.updateStrings(1, priceOrdinal, new String[] { "000699" }, null);

        updates.updateStrings(2, storedOrdinal, null, "{userid=1,searchId=9}".getBytes());
        updates.updateStrings(2, titleOrdinal, new String[] { "a", "wrinkle", "in", "cloth" }, null);
        updates.updateStrings(2, bodyOrdinal, new String[] { "quick", "red", "fox" }, null);
        updates.updateStrings(2, priceOrdinal, new String[] { "000599" }, null);

        updates.updateStrings(3, storedOrdinal, null, "{userid=2,searchId=15}".getBytes());
        updates.updateStrings(3, titleOrdinal, new String[] { "a", "wrinkle", "in", "cloth" }, null);
        updates.updateStrings(3, bodyOrdinal, new String[] { "quick", "red", "fox" }, null);
        updates.updateStrings(3, priceOrdinal, new String[] { "001599" }, null);

        index.update(updates);
        index.flush();
        System.out.println(index.summary());
        index.compact();


        LABSearchCombinator<CachedFieldValue> combinator = LABSearchOdometer.buildOdometer(index,
            Arrays.asList("title", "body"),
            Arrays.asList(Arrays.asList("*"), Arrays.asList("brown","fox",null)),
            true);

        System.out.println(combinator.combinations());
        while (combinator.hasNext()) {
            List<CachedFieldValue> next = combinator.next();
            RoaringBitmap answer = index.andFieldValues(next, null, null);
            System.out.println(answer.getCardinality() + " had " + next);
            index.storedValues(answer, storedOrdinal, (index1, value) -> {
                System.out.println(index1 + "->" + value.getLong(0));
                return true;
            });
        }

    }


}