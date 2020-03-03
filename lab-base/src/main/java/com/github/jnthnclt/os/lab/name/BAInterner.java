package com.github.jnthnclt.os.lab.name;


import com.github.jnthnclt.os.lab.collections.bah.ConcurrentBAHash;

/**
 *
 * @author jonathan.colt
 */
public class BAInterner {

    private final ConcurrentBAHash<byte[]> interned = new ConcurrentBAHash<>(3, false, 1024);

    public byte[] intern(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }
        byte[] got = interned.get(bytes, offset, length);
        if (got != null) {
            return got;
        }
        byte[] copy = new byte[length];
        System.arraycopy(bytes, offset, copy, 0, length);
        interned.put(copy, copy);
        return copy;
    }

    public int size() {
        return interned.size();
    }
}
