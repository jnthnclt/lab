package org.colt.os.lab.bitmaps;

/**
 * Created by jonathan.colt on 6/28/17.
 */
public class LABStripingLocksProvider {

    private final LABStripingLock[] locks;

    public LABStripingLocksProvider(int numLocks) {
        locks = new LABStripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new LABStripingLock();
        }
    }

    public Object lock(byte[] toLock, int seed) {
        return locks[Math.abs((LABIndexKey.hashCode(toLock, 0, toLock.length) ^ seed) % locks.length)];
    }

    static private class LABStripingLock {
    }
}