package com.github.jnthnclt.os.lab.core.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;

/**
 * @author jonathan.colt
 */
public class ValueIndexConfig {

    public final String primaryName;
    public final int entriesBetweenLeaps;
    public final long maxHeapPressureInBytes;
    public final long splitWhenKeysTotalExceedsNBytes;
    public final long splitWhenValuesTotalExceedsNBytes;
    public final long splitWhenValuesAndKeysTotalExceedsNBytes;
    public final String formatTransformerProviderName;
    public final String rawhideName;
    public final String rawEntryFormatName;
    public final int entryLengthPower;
    public final LABHashIndexType hashIndexType;
    public final double hashIndexLoadFactor;
    public final boolean hashIndexEnabled;

    @JsonCreator
    public ValueIndexConfig(@JsonProperty("primaryName") String primaryName,
        @JsonProperty("entriesBetweenLeaps") int entriesBetweenLeaps,
        @JsonProperty("maxHeapPressureInBytes") long maxHeapPressureInBytes,
        @JsonProperty("splitWhenKeysTotalExceedsNBytes") long splitWhenKeysTotalExceedsNBytes,
        @JsonProperty("splitWhenValuesTotalExceedsNBytes") long splitWhenValuesTotalExceedsNBytes,
        @JsonProperty("splitWhenValuesAndKeysTotalExceedsNBytes") long splitWhenValuesAndKeysTotalExceedsNBytes,
        @JsonProperty("formatTransformerProviderName") String formatTransformerProviderName,
        @JsonProperty("rawhideName") String rawhideName,
        @JsonProperty("rawEntryFormatName") String rawEntryFormatName,
        @JsonProperty("entryLengthPower") int entryLengthPower,
        @JsonProperty("hashIndexType") LABHashIndexType hashIndexType,
        @JsonProperty("hashIndexLoadFactor") double hashIndexLoadFactor,
        @JsonProperty("hashIndexEnabled") boolean hashIndexEnabled) {

        this.primaryName = primaryName;
        this.entriesBetweenLeaps = entriesBetweenLeaps;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.splitWhenKeysTotalExceedsNBytes = splitWhenKeysTotalExceedsNBytes;
        this.splitWhenValuesTotalExceedsNBytes = splitWhenValuesTotalExceedsNBytes;
        this.splitWhenValuesAndKeysTotalExceedsNBytes = splitWhenValuesAndKeysTotalExceedsNBytes;
        this.formatTransformerProviderName = formatTransformerProviderName;
        this.rawhideName = rawhideName;
        this.rawEntryFormatName = rawEntryFormatName;
        this.entryLengthPower = entryLengthPower;
        this.hashIndexType = hashIndexType;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.hashIndexEnabled = hashIndexEnabled;
    }

}
