package com.github.jnthnclt.os.lab.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jnthnclt.os.lab.core.api.JournalStream;
import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABKeyValueRawhide;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.core.guts.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.guts.Leaps;
import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABAppendOnlyAllocator;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMap;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABConcurrentSkipListMemory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import com.github.jnthnclt.os.lab.core.api.RawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.guts.LABCSLMIndex;
import com.github.jnthnclt.os.lab.core.guts.allocators.LABIndexableMemory;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

/**
 * @author jonathan.colt
 */
public class LABEnvironment {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final File labRoot;
    private final LABStats stats;
    private final LABFiles labFiles;
    private final ExecutorService scheduler;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final LABHeapPressure labHeapPressure;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final boolean useIndexableMemory;
    private final boolean fsyncFileRenames;

    private final String metaName;
    private final LABMeta meta;

    private final String walName;
    private final LABWAL wal;
    private final Map<String, Rawhide> rawhideRegistry = Maps.newConcurrentMap();
    private final Map<String, RawEntryFormat> rawEntryFormatRegistry = Maps.newConcurrentMap();

    private final StripingBolBufferLocks stripingBolBufferLocks;

    public static ExecutorService buildLABHeapSchedulerThreadPool(int count) {
        return LABBoundedExecutor.newBoundedExecutor(count, "lap-heap");
    }

    public static ExecutorService buildLABSchedulerThreadPool(int count) {
        return LABBoundedExecutor.newBoundedExecutor(count, "lap-scheduler");
    }

    public static ExecutorService buildLABCompactorThreadPool(int count) {
        return LABBoundedExecutor.newBoundedExecutor(count, "lap-compact");
    }

    public static ExecutorService buildLABDestroyThreadPool(int count) {
        return LABBoundedExecutor.newBoundedExecutor(count, "lap-destroy");
    }

    public static LRUConcurrentBAHLinkedHash<Leaps> buildLeapsCache(int maxCapacity, int concurrency) {
        return new LRUConcurrentBAHLinkedHash<>(10, maxCapacity, 0.5f, true, concurrency);
    }

    public LABEnvironment(LABStats stats,
        LABFiles labFiles,
        ExecutorService scheduler,
        ExecutorService compact,
        final ExecutorService destroy,
        LABWALConfig walConfig,
        File labRoot,
        LABHeapPressure heapPressure,
        int minMergeDebt,
        int maxMergeDebt,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        StripingBolBufferLocks bolBufferLocks,
        boolean useIndexableMemory,
        boolean fsyncFileRenames) throws Exception {

        register(LABKeyValueRawhide.NAME, LABKeyValueRawhide.SINGLETON);
        register(LABRawhide.NAME, LABRawhide.SINGLETON);
        register(MemoryRawEntryFormat.NAME, MemoryRawEntryFormat.SINGLETON);

        this.stats = stats;
        this.labFiles = labFiles;
        this.scheduler = scheduler;
        this.compact = compact;
        this.destroy = destroy;
        this.labRoot = labRoot;
        this.labHeapPressure = heapPressure;
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;
        this.leapsCache = leapsCache;

        if (walConfig != null) {
            this.metaName = walConfig.metaName;
            this.meta = new LABMeta(new File(labRoot, walConfig.metaName));
            this.walName = walConfig.walName;
            this.wal = new LABWAL(stats,
                new File(labRoot, walName),
                walConfig.maxWALSizeInBytes,
                walConfig.maxEntriesPerWAL,
                walConfig.maxEntrySizeInBytes,
                walConfig.maxValueIndexHeapPressureOverride
            );
        } else {
            this.metaName = null;
            this.meta = null;
            this.walName = null;
            this.wal = null;
        }
        this.useIndexableMemory = useIndexableMemory;
        this.fsyncFileRenames = fsyncFileRenames;
        this.stripingBolBufferLocks = bolBufferLocks;
    }

    LABWAL getLabWAL() {
        return wal;
    }

    public void register(String name, Rawhide rawhide) {
        Rawhide had = rawhideRegistry.putIfAbsent(name, rawhide);
        if (had != null) {
            throw new IllegalArgumentException("Rawhide:" + had + " is already register under the name:" + name);
        }
    }

    Rawhide rawhide(String name) {
        return rawhideRegistry.get(name);
    }

    public void register(String name, RawEntryFormat rawEntryFormat) {
        RawEntryFormat had = rawEntryFormatRegistry.putIfAbsent(name, rawEntryFormat);
        if (had != null) {
            throw new IllegalArgumentException("RawEntryFormat:" + had + " is already register under the name:" + name);
        }
    }

    RawEntryFormat rawEntryFormat(String name) {
        return rawEntryFormatRegistry.get(name);
    }

    public void open() throws Exception {
        open((JournalStream) null);
    }

    public void open(JournalStream journalStream) throws Exception {
        if (this.wal != null) {
            this.wal.open(this, journalStream);
        }
    }

    public void close() throws Exception {
        if (this.wal != null) {
            this.wal.close(this);
        }
        if (this.meta != null) {
            this.meta.close();
        }
    }

    public List<String> list() {
        List<String> indexes = Lists.newArrayList();
        File[] files = labRoot.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && !file.getName().equals(walName) && !file.getName().equals(metaName)) {
                    indexes.add(file.getName());
                }
            }
        }
        return indexes;
    }

    public boolean exists(String name) {
        if (name.equals(walName)) {
            return false;
        }
        if (name.equals(metaName)) {
            return false;
        }
        File labDir = new File(labRoot, name);
        return labDir.exists();
    }

    ValueIndexConfig valueIndexConfig(byte[] valueIndexId) throws Exception {
        if (meta == null) {
            throw new IllegalStateException("This environment doesn't support journaled appends");
        }
        ValueIndexConfig config = meta.get(valueIndexId, (metaValue) -> {
            if (metaValue != null) {
                return MAPPER.readValue(metaValue.copy(), ValueIndexConfig.class);
            } else {
                return null;
            }
        });
        if (config == null) {
            // Fallback to the old way :(
            String primaryName = new String(valueIndexId, StandardCharsets.UTF_8);
            File configFile = new File(labRoot, primaryName + ".json");
            if (configFile.exists()) {
                config = MAPPER.readValue(configFile, ValueIndexConfig.class);
                meta.append(valueIndexId, MAPPER.writeValueAsBytes(config), true);
                FileUtils.deleteQuietly(configFile);
            } else {
                throw new IllegalStateException("There is no config for lab value index:" + new String(valueIndexId, StandardCharsets.UTF_8));
            }
        }
        return config;
    }

    public ValueIndex<byte[]> open(ValueIndexConfig config) throws Exception {

        if (config.primaryName.equals(walName)) {
            throw new IllegalStateException("primaryName:" + config.primaryName + " cannot collide with walName");
        }
        if (config.primaryName.equals(metaName)) {
            throw new IllegalStateException("primaryName:" + config.primaryName + " cannot collide with metaName");
        }

        Rawhide rawhide = rawhideRegistry.get(config.rawhideName);

        byte[] valueIndexId = config.primaryName.getBytes(StandardCharsets.UTF_8);
        if (meta != null) {

            byte[] configAsBytes = MAPPER.writeValueAsBytes(config);
            boolean equal = meta.get(valueIndexId, (metaValue) -> {
                if (metaValue == null) {
                    return false;
                } else {
                    boolean equal1 = metaValue.length == configAsBytes.length;
                    if (equal1) {
                        for (int i = 0; i < metaValue.length; i++) {
                            if (configAsBytes[i] != metaValue.get(i)) {
                                equal1 = false;
                                break;
                            }
                        }
                    }
                    return equal1;
                }
            });

            if (!equal) {
                meta.append(valueIndexId, configAsBytes, true);
            }
        }

        LABIndexProvider<BolBuffer, BolBuffer> indexProvider = (rawhide1, poweredUpToHint) -> {
            if (useIndexableMemory && config.entryLengthPower > 0) {
                LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator(config.primaryName,
                    Math.max(config.entryLengthPower, (poweredUpToHint - config.entryLengthPower) / 2)
                );
                LABIndexableMemory memory = new LABIndexableMemory(allocator);
                LABConcurrentSkipListMemory skipListMemory = new LABConcurrentSkipListMemory(rawhide1, memory);
                return new LABConcurrentSkipListMap(stats, skipListMemory, stripingBolBufferLocks);
            } else {
                return new LABCSLMIndex(rawhide1, stripingBolBufferLocks);
            }
        };

        return new LAB(stats,
            config.rawhideName,
            rawhide,
            scheduler,
            compact,
            destroy,
            labRoot,
            wal,
            valueIndexId,
            config.primaryName,
            config.entriesBetweenLeaps,
            labHeapPressure,
            config.maxHeapPressureInBytes,
            minMergeDebt,
            maxMergeDebt,
            config.splitWhenKeysTotalExceedsNBytes,
            config.splitWhenValuesTotalExceedsNBytes,
            config.splitWhenValuesAndKeysTotalExceedsNBytes,
            leapsCache,
            indexProvider,
            fsyncFileRenames,
            config.hashIndexType,
            config.hashIndexLoadFactor,
            config.hashIndexEnabled,
            config.deleteTombstonedVersionsAfterMillis,
            labFiles);

    }

    private static final byte[] EMPTY = new byte[0];

    public boolean rename(String oldName, String newName, boolean flush) throws Exception {
        File oldFileName = new File(labRoot, oldName);
        File newFileName = new File(labRoot, newName);
        if (oldFileName.exists()) {
            byte[] oldKey = oldName.getBytes(StandardCharsets.UTF_8);
            if (meta != null) {
                byte[] value = meta.get(oldKey, BolBuffer::copy);
                byte[] newKey = newName.getBytes(StandardCharsets.UTF_8);
                meta.append(newKey, value, flush);
            }
            Files.move(oldFileName.toPath(), newFileName.toPath(), StandardCopyOption.ATOMIC_MOVE);
            FileUtils.deleteDirectory(oldFileName);
            if (meta != null) {
                meta.append(oldKey, EMPTY, flush);
            }
            return true;
        } else {
            return false;
        }
    }

    public void remove(String primaryName, boolean flush) throws Exception {
        File fileName = new File(labRoot, primaryName);
        if (fileName.exists()) {
            FileUtils.deleteDirectory(fileName);
            if (meta != null) {
                byte[] metaKey = primaryName.getBytes(StandardCharsets.UTF_8);
                meta.append(metaKey, EMPTY, flush);
            }
        }
    }

    public void delete() throws IOException {
        FileUtils.deleteDirectory(labRoot);
    }

    public void shutdown() throws InterruptedException {
        compact.shutdown();
        destroy.shutdown();
        compact.awaitTermination(30, TimeUnit.SECONDS);
        destroy.awaitTermination(30, TimeUnit.SECONDS);
    }
}
