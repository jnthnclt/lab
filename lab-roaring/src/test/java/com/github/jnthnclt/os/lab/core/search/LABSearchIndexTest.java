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
import org.testng.Assert;
import org.testng.annotations.Test;

public class LABSearchIndexTest {

    @Test
    public void helloSearchTest() throws Exception {

        File root = Files.createTempDir();
        System.out.println(root.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABIndexProvider labIndexProvider = new LABIndexProvider(globalHeapCostInBytes, stats);
        LABSearchIndex index = new LABSearchIndex(labIndexProvider, root);

        LABSearchIndexUpdates updates = new LABSearchIndexUpdates();

        int storedOrdinal = index.fieldOrdinal("stored");
        int titleOrdinal = index.fieldOrdinal("title");
        int bodyOrdinal = index.fieldOrdinal("body");
        int priceOrdinal = index.fieldOrdinal("price");
        int countOrdinal = index.fieldOrdinal("count");
        int sizeOrdinal = index.fieldOrdinal("size");

        Assert.assertEquals(index.fields().size(), 6);

        long timestamp = System.currentTimeMillis();

        updates.store(0, storedOrdinal, timestamp, "{userid=1,searchId=5}".getBytes());
        updates.updateStrings(0, titleOrdinal, timestamp,  "a", "wrinkle", "in", "time" );
        updates.updateStrings(0, bodyOrdinal, timestamp,  "quick", "brown", "fox" );
        updates.updateStrings(0, priceOrdinal, timestamp,  "000799" );
        updates.updateInts(0, countOrdinal, timestamp,  1,2,3,4 );
        updates.updateLongs(0, sizeOrdinal, timestamp,  9,8,7,6 );

        updates.store(1, storedOrdinal, timestamp, "{userid=1,searchId=7}".getBytes());
        updates.updateStrings(1, titleOrdinal, timestamp,  "a", "crease", "in", "time" );
        updates.updateStrings(1, bodyOrdinal, timestamp,  "slow", "red", "fox" );
        updates.updateStrings(1, priceOrdinal, timestamp,  "000699" );

        updates.store(2, storedOrdinal, timestamp, "{userid=1,searchId=9}".getBytes());
        updates.updateStrings(2, titleOrdinal, timestamp,  "a", "wrinkle", "in", "cloth" );
        updates.updateStrings(2, bodyOrdinal, timestamp,  "quick", "red", "fox" );
        updates.updateStrings(2, priceOrdinal, timestamp,  "000599" );

        updates.store(3, storedOrdinal, timestamp, "{userid=1,searchId=19}".getBytes());
        updates.updateStrings(3, titleOrdinal, timestamp,  "a", "wrinkle", "in", "cloth" );
        updates.updateStrings(3, bodyOrdinal, timestamp,  "quick", "red", "fox" );
        updates.updateStrings(3, priceOrdinal, timestamp,  "001599" );

        index.update(updates, false);
        index.flush();

        Assert.assertEquals(4,updates.size());

        updates.clear();
        System.out.println(index.summary());
        index.compact();

        Assert.assertEquals(index.bitmap(bodyOrdinal,"fox").getCardinality(),4);
        Assert.assertEquals(index.bitmap(bodyOrdinal,"slow").getCardinality(),1);
        Assert.assertNull(index.bitmap(countOrdinal,"dFDSFKSDH"));

        Assert.assertEquals(index.bitmap(countOrdinal,1).getCardinality(),1);
        Assert.assertNull(index.bitmap(countOrdinal,10));

        Assert.assertEquals(index.bitmap(sizeOrdinal,9L).getCardinality(),1);
        Assert.assertNull(index.bitmap(sizeOrdinal,90L));

        RoaringBitmap bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("body", "fox")), null, null);
        Assert.assertEquals(bitmap.getCardinality(),4);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("body", "slow")), null, null);
        Assert.assertEquals(bitmap.getCardinality(),1);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("body", "dFDSFKSDH")), null, null);
        Assert.assertEquals(bitmap.getCardinality(),0);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("count", 1)), null, null);
        Assert.assertEquals(bitmap.getCardinality(),1);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("count", 10)), null, null);
        Assert.assertEquals(bitmap.getCardinality(),0);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("size", 9L)), null, null);
        Assert.assertEquals(bitmap.getCardinality(),1);

        bitmap = LABSearch.andFieldValues(index, Arrays.asList(new CachedFieldValue("size", 90L)), null, null);
        Assert.assertEquals(bitmap.getCardinality(),0);


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