package com.github.jnthnclt.os.lab.core.search;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class LABSearchIndexUpdates {

    static Function<Integer, Map<Long, byte[]>> createMap2 = s -> Maps.newHashMap();
    static Function<Integer, Map<String, List<Long>>> createMap1 = s -> Maps.newHashMap();
    static Function<String, List<Long>> createListLongs = s -> Lists.newArrayList();


    public final Set<Long> guids = Sets.newHashSet();
    public final Map<Long,Long> updateTimestampMillis = Maps.newHashMap();
    public final Map<Integer, Map<Long, byte[]>> fieldNameGuidStoredFieldValue = Maps.newHashMap();
    public final Map<Integer, Map<String, List<Long>>> fieldNameFieldValueGuids = Maps.newHashMap();

    public void updateInts(long guid,
        int fieldNameOrdinal,
        int[] fieldValues,
        byte[] storedFieldValue
    ) {
        String[] strings = null;
        if (fieldValues != null) {
            strings = new String[fieldValues.length];
            for (int i = 0; i < fieldValues.length; i++) {
                strings[i] = String.valueOf(fieldValues[i]);
            }
        }
        updateStrings(guid, fieldNameOrdinal, strings, storedFieldValue);
    }

    public void updateStrings(long guid,
        int fieldNameOrdinal,
        String[] fieldValues,
        byte[] storedFieldValue
    ) {

        guids.add(guid);
        if (fieldValues != null) {
            for (int j = 0; j < fieldValues.length; j++) {
                String fieldValue = fieldValues[j];
                if (fieldValue != null) {

                    Map<String, List<Long>> fieldValue_guids = this.fieldNameFieldValueGuids.computeIfAbsent(fieldNameOrdinal, createMap1);
                    fieldValue_guids.computeIfAbsent(fieldValue, createListLongs).add(guid);
                }
            }
        }

        if (storedFieldValue != null) {
            fieldNameGuidStoredFieldValue.computeIfAbsent(fieldNameOrdinal, createMap2).put(guid, storedFieldValue);
        }
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
