package com.github.jnthnclt.os.lab.core.search;

import com.github.jnthnclt.os.lab.collections.bah.BAHEqualer;
import com.github.jnthnclt.os.lab.collections.bah.BAHMapState;
import com.github.jnthnclt.os.lab.collections.bah.BAHash;
import com.github.jnthnclt.os.lab.collections.bah.BAHasher;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class LABSearchIndexUpdates {

    static Function<Integer, BAHash<byte[]>> bytesBAHashFunction = k -> {
        return new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
    };
    static Function<Integer, BAHash<List<Long>>> listLongsBAHashFunction = k -> {
        return new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
    };


    public final Set<Long> guids = Sets.newHashSet();
    public final Map<Long, Long> updateTimestampMillis = Maps.newHashMap();
    public final Map<Integer, BAHash<byte[]>> fieldNameGuidStoredFieldValue = Maps.newHashMap();
    public final Map<Integer, BAHash<List<Long>>> fieldNameFieldValueGuids = Maps.newHashMap();

    public boolean updateObject(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        Object fieldValue) {

        if (fieldValue instanceof Long) {
            update(guid, fieldNameOrdinal, timestampMillis, UIO.longBytes((Long) fieldValue));
            return true;
        } else if (fieldValue instanceof Integer) {
            update(guid, fieldNameOrdinal, timestampMillis, UIO.longBytes((Integer) fieldValue));
            return true;
        } else if (fieldValue instanceof String) {
            update(guid, fieldNameOrdinal, timestampMillis, ((String)fieldValue).getBytes());
            return true;
        } else if (fieldValue instanceof int[]) {
            updateInts(guid, fieldNameOrdinal, timestampMillis, ((int[])fieldValue));
            return true;
        } else if (fieldValue instanceof long[]) {
            updateLongs(guid, fieldNameOrdinal, timestampMillis, ((long[])fieldValue));
            return true;
        } else if (fieldValue instanceof String[]) {
            updateStrings(guid, fieldNameOrdinal, timestampMillis, ((String[])fieldValue));
            return true;
        }
        return false;
    }

    public void updateStrings(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        String... fieldValues) {

        byte[][] bytes = null;
        if (fieldValues != null) {
            bytes = new byte[fieldValues.length][];
            for (int i = 0; i < fieldValues.length; i++) {
                bytes[i] =fieldValues[i].getBytes(StandardCharsets.UTF_8);
            }
        }
        update(guid, fieldNameOrdinal, timestampMillis, bytes);
    }

    public void updateInts(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        int... fieldValues) {

        byte[][] bytes = null;
        if (fieldValues != null) {
            bytes = new byte[fieldValues.length][];
            for (int i = 0; i < fieldValues.length; i++) {
                bytes[i] = UIO.intBytes(fieldValues[i], new byte[4],0);
            }
        }
        update(guid, fieldNameOrdinal, timestampMillis, bytes);
    }

    public void updateLongs(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        long... fieldValues) {

        byte[][] bytes = null;
        if (fieldValues != null) {
            bytes = new byte[fieldValues.length][];
            for (int i = 0; i < fieldValues.length; i++) {
                bytes[i] = UIO.longBytes(fieldValues[i], new byte[8],0);
            }
        }
        update(guid, fieldNameOrdinal, timestampMillis, bytes);
    }



    synchronized public void update(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        byte[]... fieldValues
    ) {
        updateTimestampMillis.put(guid, timestampMillis);
        guids.add(guid);
        if (fieldValues != null) {
            for (int j = 0; j < fieldValues.length; j++) {
                byte[] fieldValue = fieldValues[j];
                if (fieldValue != null) {

                    BAHash<List<Long>> fieldValue_guids = this.fieldNameFieldValueGuids.computeIfAbsent(fieldNameOrdinal, listLongsBAHashFunction);
                    List<Long> guids = fieldValue_guids.get(fieldValue, 0, fieldValue.length);
                    if (guids == null) {
                        guids = Lists.newArrayList();
                        fieldValue_guids.put(fieldValue, guids);
                    }
                    guids.add(guid);
                }
            }
        }
    }

    synchronized public void store(long guid,
        int fieldNameOrdinal,
        long timestampMillis,
        byte[] storedFieldValue
    ) {
        updateTimestampMillis.put(guid, timestampMillis);
        guids.add(guid);
        fieldNameGuidStoredFieldValue.computeIfAbsent(fieldNameOrdinal, bytesBAHashFunction).put(UIO.longBytes(guid), storedFieldValue);
    }

    public void clear() {
        guids.clear();
        fieldNameFieldValueGuids.clear();
        fieldNameGuidStoredFieldValue.clear();
    }

    public int size() {
        return guids.size();
    }
}
