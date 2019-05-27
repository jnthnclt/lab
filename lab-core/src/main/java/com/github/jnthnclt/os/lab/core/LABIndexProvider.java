package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.MemoryRawEntryFormat;
import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.api.rawhide.LABRawhide;
import com.github.jnthnclt.os.lab.core.guts.LABHashIndexType;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class LABIndexProvider<T> {

    private final LABStats stats;
    private final LABEnvironmentBuilder environmentBuilder;
    private final LABHeapPressure labHeapPressure;

    public LABIndexProvider(LABStats stats,
                            LABHeapPressureBuilder heapPressureBuilder,
                            LABEnvironmentBuilder environmentBuilder) {
        this.stats = stats;
        this.environmentBuilder = environmentBuilder;
        labHeapPressure = heapPressureBuilder.build(stats);
    }

    public void destroyIndex(File root, String name) throws Exception {
        FileUtils.forceDelete(new File(root, name));
    }

    public ValueIndex<byte[]> buildIndex(File root, ValueIndexConfig indexConfig) throws Exception {
        LABEnvironment environment = environmentBuilder.build(stats, root, labHeapPressure);
        return environment.open(indexConfig);
    }
}
