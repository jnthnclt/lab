package org.colt.os.lab.bitmaps;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public class LABIndexKeyInterner {

    private final WeakHashMap<LABIndexKey, WeakReference<LABIndexKey>>[] pools;
    private final LABIndexKey[] keys;
    private final boolean enabled;

    public LABIndexKeyInterner(boolean enabled) {
        int concurencyLevel = 1024;
        this.keys = new LABIndexKey[concurencyLevel]; // TODO config
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new LABIndexKey(new byte[0]);
        }
        this.pools = new WeakHashMap[concurencyLevel];
        for (int i = 0; i < pools.length; i++) {
            pools[i] = new WeakHashMap<>();
        }
        this.enabled = enabled;
    }

    public LABIndexKey create(byte[] bytes) {
        return new LABIndexKey(bytes);
    }

    public LABIndexKey intern(byte[] bytes) {
        if (!enabled) {
            return create(bytes);
        } else {
            return doIntern(bytes, 0, bytes.length);
        }
    }

    public LABIndexKey intern(byte[] bytes, int offset, int length) {
        if (!enabled) {
            byte[] exactBytes = new byte[length];
            System.arraycopy(bytes, offset, exactBytes, 0, length);
            return create(bytes);
        }
        return doIntern(bytes, offset, length);
    }

    // He likes to watch.
    private LABIndexKey doIntern(byte[] bytes, int offset, int length) {
        int hashCode = LABIndexKey.hashCode(bytes, offset, length);
        int index = Math.abs(hashCode % keys.length);
        LABIndexKey key = keys[index];
        WeakHashMap<LABIndexKey, WeakReference<LABIndexKey>> pool = pools[index];
        synchronized (pool) {
            byte[] exactBytes = new byte[length];
            System.arraycopy(bytes, offset, exactBytes, 0, length);
            key.mutate(exactBytes, hashCode);

            LABIndexKey res;
            WeakReference<LABIndexKey> ref = pool.get(key);
            if (ref != null) {
                res = ref.get();
            } else {
                res = null;
            }
            if (res == null) {
                res = create(exactBytes);
                pool.put(res, new WeakReference<>(res));
            }
            return res;
        }
    }
}