package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.core.api.ValueIndex;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

public class LABIndex {

    private final Map<String, ValueIndex<byte[]>> indexes = Maps.newConcurrentMap();
    private final Semaphore tx = new Semaphore(Short.MAX_VALUE);
    private final File root;
    private final LABIndexProvider labIndexProvider;

    public LABIndex(File root, LABIndexProvider labIndexProvider) {
        this.root = root;
        this.labIndexProvider = labIndexProvider;
    }

    public List<String> summary() throws Exception {
        List<String> s = Lists.newArrayList();
        for (Entry<String, ValueIndex<byte[]>> stringValueIndexEntry : indexes.entrySet()) {
            ValueIndex<byte[]> valueIndex = stringValueIndexEntry.getValue();
            s.add(stringValueIndexEntry.getKey() + ": count=" + valueIndex.count() + " debt=" + valueIndex.debt());
        }
        return s;
    }

    interface LABIndexTx {
        void tx(ValueIndex[] indexes) throws Exception;
    }

    public Collection<String> getIndexNames() {
        return indexes.keySet();
    }

    public void tx(String[] names, LABIndexTx indexTx) throws Exception {
        tx.acquire();
        try {
            ValueIndex[] indexes = new ValueIndex[names.length];
            for (int i = 0; i < names.length; i++) {
                indexes[i] = getOrCreateIndex(names[i]);
            }
            indexTx.tx(indexes);
        } finally {
            tx.release();
        }
    }

    public void delete(String indexName) throws Exception {
        tx.acquire(Short.MAX_VALUE);
        try {
            if (indexes.containsKey(indexName)) {
                indexes.remove(indexName);
                labIndexProvider.destroyIndex(root, indexName);
            }
        } finally {
            tx.release(Short.MAX_VALUE);
        }
    }

    public void load() throws Exception {
        tx.acquire(Short.MAX_VALUE);
        try {
            for (File file : root.listFiles()) {
                if (file.isDirectory()) {
                    getOrCreateIndex(FilenameUtils.removeExtension(file.getName()));
                }
            }
        } finally {
            tx.release(Short.MAX_VALUE);
        }
    }

    public void delete() throws Exception {
        tx.acquire(Short.MAX_VALUE);
        try {
            FileUtils.forceDelete(root);
        } finally {
            tx.release(Short.MAX_VALUE);
        }
    }

    public long size() throws Exception {
        tx.acquire();
        try {
            return FileUtils.sizeOfDirectory(root);
        } finally {
            tx.release();
        }
    }

    public void flush() throws Exception {
        tx.acquire();
        try {
            for (Entry<String, ValueIndex<byte[]>> entry : indexes.entrySet()) {
                entry.getValue().commit(true, true);
            }
        } finally {
            tx.release();
        }
    }

    public void compact() throws Exception {
        tx.acquire();
        try {
            for (Entry<String, ValueIndex<byte[]>> entry : indexes.entrySet()) {
                compact(entry.getValue());
            }
        } finally {
            tx.release();
        }
    }

    private void compact(ValueIndex<byte[]> index) throws Exception {

        List<Future<Object>> futures = index.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
    }

    private ValueIndex<byte[]> getOrCreateIndex(String name) throws Exception {
        ValueIndex<byte[]> got = indexes.get(name);
        if (got == null) {
            synchronized (indexes) {
                got = indexes.get(name);
                if (got == null) {
                    got = labIndexProvider.buildIndex(root, name);
                    indexes.put(name, got);
                }
            }
        }
        return got;
    }
}
