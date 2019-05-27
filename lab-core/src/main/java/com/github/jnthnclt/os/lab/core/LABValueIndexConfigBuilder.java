package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;

public class LABValueIndexConfigBuilder {

    private final String name;

    public int entriesBetweenLeaps = 1024 * 4;
    public long maxHeapPressureInBytes = 1024 * 1024 * 512;
    public long splitWhenKeysTotalExceedsNBytes = -1;
    public long splitWhenValuesTotalExceedsNBytes = -1;
    public long splitWhenValuesAndKeysTotalExceedsNBytes = 128 * 1024 * 1024;
    public String rawhideName = LABRawhide.NAME;
    public String rawEntryFormatName = MemoryRawEntryFormat.NAME;
    public int entryLengthPower = 28;
    public LABHashIndexType hashIndexType = LABHashIndexType.fibCuckoo;
    public double hashIndexLoadFactor = 2d;
    public boolean hashIndexEnabled = true;
    public long deleteTombstonedVersionsAfterMillis = Long.MAX_VALUE;

    public LABValueIndexConfigBuilder(String name) {
        this.name = name;
    }

    public ValueIndexConfig build() {
        return new ValueIndexConfig(name,
                entriesBetweenLeaps,
                maxHeapPressureInBytes,
                splitWhenKeysTotalExceedsNBytes,
                splitWhenValuesTotalExceedsNBytes,
                splitWhenValuesAndKeysTotalExceedsNBytes,
                "deprecated",
                rawhideName,
                rawEntryFormatName,
                entryLengthPower,
                hashIndexType,
                hashIndexLoadFactor,
                hashIndexEnabled,
                deleteTombstonedVersionsAfterMillis);
    }

    public LABValueIndexConfigBuilder setSplitWhenValuesAndKeysTotalExceedsNBytes(int splitWhenValuesAndKeysTotalExceedsNBytes) {
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        return this;
    }
}
