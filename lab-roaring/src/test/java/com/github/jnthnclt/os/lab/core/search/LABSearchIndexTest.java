package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
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

        int idOrdinal = index.fieldOrdinal("id");
        int titleOrdinal = index.fieldOrdinal("title");
        int bodyOrdinal = index.fieldOrdinal("body");

        updates.update(0, idOrdinal, null, UIO.longBytes(0));
        updates.update(0, titleOrdinal, new String[] { "a", "wrinkle", "in", "time" }, null);
        updates.update(0, bodyOrdinal, new String[] { "quick", "brown", "fox" }, null);

        updates.update(1, idOrdinal, null, UIO.longBytes(1));
        updates.update(1, titleOrdinal, new String[] { "a", "crease", "in", "time" }, null);
        updates.update(1, bodyOrdinal, new String[] { "slow", "brown", "fox" }, null);

        updates.update(2, idOrdinal, null, UIO.longBytes(2));
        updates.update(2, titleOrdinal, new String[] { "a", "wrinkle", "in", "cloth" }, null);
        updates.update(2, bodyOrdinal, new String[] { "quick", "red", "fox" }, null);

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
            index.storedValues(answer, idOrdinal, (index1, value) -> {
                System.out.println(index1 + "->" + value.getLong(0));
                return true;
            });
        }

    }


}