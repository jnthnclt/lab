package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.jnthnclt.os.lab.core.guts.LABAppendableIndex.LOG;

public class RangeStripeSet {

    private final byte[] labId;
    private final LABStats stats;
    private final Rawhide rawhide;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final LABFiles labFiles;

    public RangeStripeSet(byte[] labId,
                          LABStats stats,
                          Rawhide rawhide,
                          LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
                          LABFiles labFiles) {
        this.labId = labId;
        this.stats = stats;
        this.rawhide = rawhide;
        this.leapsCache = leapsCache;
        this.labFiles = labFiles;
    }

    interface RangeStripesCallback {

        void loaded(long stripeId, RangeStripe rangeStripe);
    }

    public void load(File indexRoot,
                     AtomicLong largestIndexId,
                     RangeStripesCallback rangeStripesCallback) throws Exception {

        File[] stripeDirs = indexRoot.listFiles();
        if (stripeDirs != null) {
            Map<File, RangeStripe> stripes = new HashMap<>();
            for (File stripeDir : stripeDirs) {
                RangeStripe stripe = loadStripe(stripeDir, largestIndexId);
                if (stripe != null) {
                    stripes.put(stripeDir, stripe);
                }
            }

            @SuppressWarnings("unchecked")
            Map.Entry<File, RangeStripe>[] entries = stripes.entrySet().toArray(new Map.Entry[0]);
            for (int i = 0; i < entries.length; i++) {
                if (entries[i] == null) {
                    continue;
                }
                for (int j = i + 1; j < entries.length; j++) {
                    if (entries[j] == null) {
                        continue;
                    }
                    if (entries[i].getValue().keyRange.contains(entries[j].getValue().keyRange)) {
                        FileUtils.forceDelete(entries[j].getKey());
                        entries[j] = null;
                    }
                }
            }

            for (Map.Entry<File, RangeStripe> entry : entries) {
                if (entry != null) {
                    long stripeId = Long.parseLong(entry.getKey().getName());
                    rangeStripesCallback.loaded(stripeId, entry.getValue());
                }
            }
        }
    }


    public RangeStripe loadStripe(File stripeRoot, AtomicLong largestIndexId) throws Exception {
        if (stripeRoot.isDirectory()) {
            File activeDir = new File(stripeRoot, "active");
            if (activeDir.exists()) {
                TreeSet<IndexRangeId> ranges = new TreeSet<>();
                File[] listFiles = activeDir.listFiles();
                if (listFiles != null) {
                    for (File file : listFiles) {
                        String rawRange = file.getName();
                        String[] range = rawRange.split("-");
                        long start = Long.parseLong(range[0]);
                        long end = Long.parseLong(range[1]);
                        long generation = Long.parseLong(range[2]);

                        ranges.add(new IndexRangeId(start, end, generation));
                        if (largestIndexId.get() < end) {
                            largestIndexId.set(end); //??
                        }
                    }
                }

                IndexRangeId active = null;
                TreeSet<IndexRangeId> remove = new TreeSet<>();
                for (IndexRangeId range : ranges) {
                    if (active == null || !active.intersects(range)) {
                        active = range;
                    } else {
                        LOG.debug("Destroying index for overlaping range:{}", range);
                        remove.add(range);
                    }
                }

                for (IndexRangeId range : remove) {
                    File file = range.toFile(activeDir);
                    FileUtils.deleteQuietly(file);
                }
                ranges.removeAll(remove);

                /**
                 0/1-1-0 a,b,c,d -append
                 0/2-2-0 x,y,z - append
                 0/1-2-1 a,b,c,d,x,y,z - merge
                 0/3-3-0 -a,-b - append
                 0/1-3-2 c,d,x,y,z - merge
                 - split
                 1/1-3-2 c,d
                 2/1-3-2 x,y,z
                 - delete 0/*
                 */
                CompactableIndexes mergeableIndexes = new CompactableIndexes(stats, rawhide);
                KeyRange keyRange = null;
                for (IndexRangeId range : ranges) {
                    File file = range.toFile(activeDir);
                    if (file.length() == 0) {
                        file.delete();
                        continue;
                    }
                    ReadOnlyFile indexFile = new ReadOnlyFile(file);
                    ReadOnlyIndex lab = new ReadOnlyIndex(range, indexFile, rawhide, leapsCache);
                    if (lab.minKey() != null && lab.maxKey() != null) {
                        if (keyRange == null) {
                            keyRange = new KeyRange(rawhide.getKeyComparator(), lab.minKey(), lab.maxKey());
                        } else {
                            keyRange = keyRange.join(lab.minKey(), lab.maxKey());
                        }
                        if (!mergeableIndexes.append(lab)) {
                            throw new IllegalStateException("Bueller");
                        }
                        if (labFiles != null) {
                            labFiles.add(labId, -1, file);
                        }
                    } else {
                        indexFile.close();
                        indexFile.delete();
                    }
                }
                if (keyRange != null) {
                    return new RangeStripe(keyRange, mergeableIndexes);
                }
            }
        }
        return null;
    }
}
