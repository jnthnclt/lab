package com.github.jnthnclt.os.lab.core.guts.allocators;

import com.github.jnthnclt.os.lab.core.guts.StripingBolBufferLocks;
import com.github.jnthnclt.os.lab.core.guts.api.Next;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.api.FormatTransformer;
import com.github.jnthnclt.os.lab.core.guts.LABIndex;
import com.github.jnthnclt.os.lab.core.guts.api.RawEntryStream;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;

public class LABConcurrentSkipListMap implements LABIndex<BolBuffer, BolBuffer> {

    private static final long BASE_HEADER = -3;
    private static final long SELF = -2;
    private static final long NIL = -1;

    private transient volatile int head;

    private static final AtomicIntegerFieldUpdater<LABConcurrentSkipListMap> headUpdater = AtomicIntegerFieldUpdater
        .newUpdater(LABConcurrentSkipListMap.class, "head");

    final LABConcurrentSkipListMemory memory;

    private void initialize() throws InterruptedException {
        int headNode = allocateNode(NIL, BASE_HEADER, NIL);
        head = allocateIndex(headNode, (int) NIL, (int) NIL, 1);
    }

    private boolean casHead(int cmp, int val) {
        return headUpdater.compareAndSet(this, cmp, val);
    }

    private final int ALL = 1024; // TODO Expose?
    private final Semaphore growSemaphore = new Semaphore(ALL);
    private volatile AtomicLongArray nodesArray = new AtomicLongArray(4 * 16);
    private final AtomicInteger freeNode = new AtomicInteger(-1);
    private final AtomicInteger allocateNode = new AtomicInteger();

    private static final int NODE_KEY_OFFSET = 0;
    private static final int NODE_VALUE_OFFSET = 1;
    private static final int NODE_REF_COUNT_OFFSET = 2;
    private static final int NODE_NEXT_OFFSET = 3;
    private static final int NODE_SIZE_IN_LONGS = 4;

    private int allocateNode(long next) throws InterruptedException {
        return allocateNode(NIL, SELF, next);
    }

    private int allocateNode(long key, long value, long nextNode) throws InterruptedException {
        int address;
        address = freeNode.get();
        while (address != -1) {
            int nextAddress = (int) nodesArray.get(address);
            if (freeNode.compareAndSet(address, nextAddress)) {
                nodesArray.set(address + NODE_KEY_OFFSET, key);
                nodesArray.set(address + NODE_VALUE_OFFSET, value);
                nodesArray.set(address + NODE_REF_COUNT_OFFSET, 0);
                nodesArray.set(address + NODE_NEXT_OFFSET, nextNode);
                return address;
            }
            address = freeNode.get();
        }
        address = allocateNode.getAndAdd(NODE_SIZE_IN_LONGS);
        if (address + NODE_SIZE_IN_LONGS >= nodesArray.length()) {

            growSemaphore.release();
            try {
                growSemaphore.acquire(ALL);
                try {
                    if (address + NODE_SIZE_IN_LONGS >= nodesArray.length()) {

                        int newSize = Math.max(nodesArray.length() + NODE_SIZE_IN_LONGS, nodesArray.length() * 2);

                        long[] newNodesArray = new long[newSize];
                        for (int i = 0; i < nodesArray.length(); i++) { // LAME!!!
                            newNodesArray[i] = nodesArray.get(i);
                        }
                        nodesArray = new AtomicLongArray(newNodesArray);
                    }
                } finally {
                    growSemaphore.release(ALL);
                }
            } finally {
                growSemaphore.acquire();
            }
        }

        nodesArray.set(address + NODE_KEY_OFFSET, key);
        nodesArray.set(address + NODE_VALUE_OFFSET, value);
        nodesArray.set(address + NODE_REF_COUNT_OFFSET, 0);
        nodesArray.set(address + NODE_NEXT_OFFSET, nextNode);
        return address;

    }

    private void freeNode(int address) throws InterruptedException {
        growSemaphore.release();
        try {
            growSemaphore.acquire(ALL);
            try {
                int freeAddress;
                do {
                    freeAddress = freeNode.get();
                    nodesArray.set(address, freeAddress);
                }
                while (!freeNode.compareAndSet(freeAddress, address));
            } finally {
                growSemaphore.release(ALL);
            }
        } finally {
            growSemaphore.acquire();
        }

    }

    long nodeKey(int address) throws InterruptedException {
        return nodesArray.get(address + NODE_KEY_OFFSET);
    }

    long nodeValue(int address) throws InterruptedException {
        return nodesArray.get(address + NODE_VALUE_OFFSET);
    }

    int nodeNext(int address) throws InterruptedException {
        return (int) nodesArray.get(address + NODE_NEXT_OFFSET);
    }

    private long acquireValue(int address) throws InterruptedException {
        long v = nodeValue(address);
        if (v != NIL && v != SELF) {
            long at = nodesArray.get(address + NODE_REF_COUNT_OFFSET);
            while (true) {
                if (at >= 0 && nodesArray.compareAndSet(address + NODE_REF_COUNT_OFFSET, at, at + 1)) {
                    break;
                } else {
                    at = nodesArray.get(address + NODE_REF_COUNT_OFFSET);
                    Thread.yield();
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                }
            }
        }
        return v;
    }

    void releaseValue(int address) throws InterruptedException {
        nodesArray.decrementAndGet(address + NODE_REF_COUNT_OFFSET);
    }

    boolean casValue(int address, LABConcurrentSkipListMemory memory, long cmp, long val) throws Exception {
        while (!nodesArray.compareAndSet(address + NODE_REF_COUNT_OFFSET, 0, -1)) {
            Thread.yield();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        try {
            boolean cas = nodesArray.compareAndSet(address + NODE_VALUE_OFFSET, cmp, val);
            if (cas) {
                stats.released.add(4 + memory.release(cmp));
            }
            return cas;
        } finally {
            nodesArray.compareAndSet(address + NODE_REF_COUNT_OFFSET, -1, 0);
        }
    }

    boolean casNext(int address, int cmp, int val) throws InterruptedException {
        return nodesArray.compareAndSet(address + NODE_NEXT_OFFSET, cmp, val);
    }

    boolean isMarker(int address) throws InterruptedException {
        return nodeValue(address) == SELF;
    }

    boolean isBaseHeader(int address) throws InterruptedException {
        return nodeValue(address) == BASE_HEADER;
    }

    boolean appendMarker(int address, int f) throws InterruptedException {
        int newF = allocateNode(f);
        if (casNext(address, f, newF)) {
            return true;
        } else {
            freeNode(newF);
            return false;
        }
    }

    void helpDelete(int address, int b, int f) throws InterruptedException {
        /*
             * Rechecking links and then doing only one of the
             * help-out stages per call tends to minimize CAS
             * interference among helping threads.
         */
        if (f == nodeNext(address) && address == nodeNext(b)) {
            if (f == NIL || nodeValue(f) != SELF) // not already marked
            {
                int newF = allocateNode(f);

                if (!casNext(address, f, newF)) {
                    freeNode(newF);
                }
            } else {
                casNext(b, address, nodeNext(f));
            }
        }
    }

    private volatile AtomicIntegerArray indexsArray = new AtomicIntegerArray(3 * 8);
    private final AtomicInteger freeIndex = new AtomicInteger(-1);
    private final AtomicInteger allocateIndex = new AtomicInteger();

    private static final int INDEX_NODE = 0;
    private static final int INDEX_DOWN = 1;
    private static final int INDEX_RIGHT = 2;
    private static final int INDEX_LEVEL = 3;
    private static final int INDEX_SIZE_IN_INTS = 4;

    private int allocateIndex(int node, int down, int right, int level) throws InterruptedException {
        int address;
        address = freeIndex.get();
        while (address != -1) {
            int nextAddress = indexsArray.get(address);
            if (freeIndex.compareAndSet(address, nextAddress)) {
                indexsArray.set(address + INDEX_NODE, node);
                indexsArray.set(address + INDEX_DOWN, down);
                indexsArray.set(address + INDEX_RIGHT, right);
                indexsArray.set(address + INDEX_LEVEL, level);
                return address;
            }
            address = freeIndex.get();
        }

        address = allocateIndex.getAndAdd(INDEX_SIZE_IN_INTS);
        int endOfAddress = address + INDEX_SIZE_IN_INTS;
        if (endOfAddress >= indexsArray.length()) {

            growSemaphore.release();
            try {
                growSemaphore.acquire(ALL);
                try {
                    if (endOfAddress >= indexsArray.length()) {
                        int newSize = (endOfAddress) * 2;
                        int[] newIndexsArray = new int[newSize];
                        for (int i = 0; i < indexsArray.length(); i++) { // LAME!!!
                            newIndexsArray[i] = indexsArray.get(i);
                        }
                        indexsArray = new AtomicIntegerArray(newIndexsArray);
                    }
                } finally {
                    growSemaphore.release(ALL);
                }
            } finally {
                growSemaphore.acquire();
            }
        }

        indexsArray.set(address + INDEX_NODE, node);
        indexsArray.set(address + INDEX_DOWN, down);
        indexsArray.set(address + INDEX_RIGHT, right);
        indexsArray.set(address + INDEX_LEVEL, level);
        return address;

    }

    private void freeIndex(int address) throws InterruptedException {
        growSemaphore.release();
        try {
            growSemaphore.acquire(ALL);
            try {
                int freeAddress;
                do {
                    freeAddress = freeIndex.get();
                    indexsArray.set(address, freeAddress);
                }
                while (!freeIndex.compareAndSet(freeAddress, address));
            } finally {
                growSemaphore.release(ALL);
            }
        } finally {
            growSemaphore.acquire();
        }

    }

    final boolean casRight(int indexAddress, int cmp, int val) {
        return indexsArray.compareAndSet(indexAddress + INDEX_RIGHT, cmp, val);
    }

    final boolean indexesDeletedNode(int indexAddress) throws InterruptedException {
        return nodeValue(indexsArray.get(indexAddress + INDEX_NODE)) == NIL;
    }

    final boolean link(int indexAddress, int succ, int newSucc) throws InterruptedException {
        int n = indexsArray.get(indexAddress + INDEX_NODE);
        indexsArray.set(newSucc + INDEX_RIGHT, succ);
        return nodeValue(n) != NIL && casRight(indexAddress, succ, newSucc);
    }

    final boolean unlink(int indexAddress, int succ) throws InterruptedException {
        int n = indexsArray.get(indexAddress + INDEX_NODE);
        if (nodeValue(n) != NIL) {
            if (casRight(indexAddress, succ, indexsArray.get(succ + INDEX_RIGHT))) {
                freeIndex(succ);
                return true;
            }
        }
        return false;
    }

    private int indexNode(int indexAddress) {
        return indexsArray.get(indexAddress + INDEX_NODE);
    }

    private int indexRight(int indexAddress) {
        return indexsArray.get(indexAddress + INDEX_RIGHT);
    }

    private int indexLevel(int indexAddress) {
        return indexsArray.get(indexAddress + INDEX_LEVEL);
    }

    private int indexDown(int indexAddress) {
        return indexsArray.get(indexAddress + INDEX_DOWN);
    }

    /* ---------------- Traversal -------------- */

    /**
     * Returns a base-level node with key strictly less than given key,
     * or the base-level header if there is no such node.  Also
     * unlinks indexes to deleted nodes found along the way.  Callers
     * rely on this side-effect of clearing indices to deleted nodes.
     *
     * @param key the key
     * @return a predecessor of key
     */
    private int findPredecessor(BolBuffer key) throws InterruptedException {
        if (key == null || key.length == -1) {
            throw new NullPointerException(); // don't postpone errors
        }
        for (; ; ) {
            for (int q = head, r = indexRight(q), d; ; ) {
                if (r != NIL) {
                    int n = indexNode(r);
                    long k = nodeKey(n);
                    if (nodeValue(n) == NIL) {
                        if (!unlink(q, r)) {
                            break;           // restart
                        }
                        r = indexRight(q);         // reread r
                        continue;
                    }
                    if (memory.compareBL(key.bytes, key.offset, key.length, k) > 0) {
                        q = r;
                        r = indexRight(r);
                        continue;
                    }
                }
                if ((d = indexDown(q)) == NIL) {
                    return indexNode(q);
                }
                q = d;
                r = indexRight(d);
            }
        }
    }

    /**
     * Returns node holding key or null if no such, clearing out any
     * deleted nodes seen along the way.  Repeatedly traverses at
     * base-level looking for key starting at predecessor returned
     * from findPredecessor, processing base-level deletions as
     * encountered. Some callers rely on this side-effect of clearing
     * deleted nodes.
     * <p>
     * Restarts occur, at traversal step centered on node n, if:
     * <p>
     * (1) After reading n's next field, n is no longer assumed
     * predecessor b's current successor, which means that
     * we don't have a consistent 3-node snapshot and so cannot
     * unlink any subsequent deleted nodes encountered.
     * <p>
     * (2) n's value field is null, indicating n is deleted, in
     * which case we help out an ongoing structural deletion
     * before retrying.  Even though there are cases where such
     * unlinking doesn't require restart, they aren't sorted out
     * here because doing so would not usually outweigh cost of
     * restarting.
     * <p>
     * (3) n is a marker or n's predecessor's value field is null,
     * indicating (among other possibilities) that
     * findPredecessor returned a deleted node. We can't unlink
     * the node because we don't know its predecessor, so rely
     * on another call to findPredecessor to notice and return
     * some earlier predecessor, which it will do. This check is
     * only strictly needed at beginning of loop, (and the
     * b.value check isn't strictly needed at all) but is done
     * each iteration to help avoid contention with other
     * threads by callers that will fail to be able to change
     * links, and so will retry anyway.
     * <p>
     * The traversal loops in doPut, doRemove, and findNear all
     * include the same three kinds of checks. And specialized
     * versions appear in findFirst, and findLast and their
     * variants. They can't easily share code because each uses the
     * reads of fields held in locals occurring in the orders they
     * were performed.
     *
     * @param key the key
     * @return node holding key, or null if no such
     */
    private int findNode(BolBuffer key) throws InterruptedException {
        if (key == null || key.length == -1) {
            throw new NullPointerException(); // don't postpone errors
        }
        outer:
        for (; ; ) {
            for (int b = findPredecessor(key), n = nodeNext(b); ; ) {
                long v;
                int c;
                if (n == NIL) {
                    break outer;
                }
                int f = nodeNext(n);
                if (n != nodeNext(b)) // inconsistent read
                {
                    break;
                }
                if ((v = nodeValue(n)) == NIL) {    // n is deleted
                    helpDelete(n, b, f);
                    break;
                }
                if (nodeValue(b) == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if ((c = memory.compareBL(key.bytes, key.offset, key.length, nodeKey(n))) == 0) {
                    return n;
                }
                if (c < 0) {
                    break outer;
                }
                b = n;
                n = f;
            }
        }
        return (int) NIL;
    }

    /**
     * Gets value for key. Almost the same as findNode, but returns
     * the found value (to avoid retries during re-reads)
     *
     * @param key the key
     * @return the value, or null if absent
     */
    private long doGet(BolBuffer key) throws InterruptedException {
        if (key == null || key.length == -1) {
            throw new NullPointerException();
        }
        outer:
        for (; ; ) {

            int b = findPredecessor(key);
            int n = nodeNext(b);
            for (; ; ) {
                long v;
                int c;
                if (n == NIL) {
                    break outer;
                }
                int f = nodeNext(n);
                if (n != nodeNext(b)) // inconsistent read
                {
                    break;
                }
                if ((v = nodeValue(n)) == NIL) {    // n is deleted
                    helpDelete(n, b, f);
                    break;
                }
                if (nodeValue(b) == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                if ((c = memory.compareBL(key.bytes, key.offset, key.length, nodeKey(n))) == 0) {
                    return v;
                }
                if (c < 0) {
                    break outer;
                }
                b = n;
                n = f;
            }
        }
        return -1;

    }

    /* ---------------- Finding and removing first element -------------- */

    /**
     * Specialized variant of findNode to get first valid node.
     *
     * @return first node or null if empty
     */
    final int findFirst() throws InterruptedException {
        for (int b, n; ; ) {
            b = indexNode(head);
            n = nodeNext(b);
            if (n == NIL) {
                return (int) NIL;
            }
            if (nodeValue(n) != NIL) {
                return n;
            }
            helpDelete(n, b, nodeNext(n));
        }
    }


    /* ---------------- Finding and removing last element -------------- */

    /**
     * Specialized version of find to get last valid node.
     *
     * @return last node or null if empty
     */
    final int findLast() throws InterruptedException {
        /*
         * findPredecessor can't be used to traverse index level
         * because this doesn't use comparisons.  So traversals of
         * both levels are folded together.
         */
        int q = head;
        for (; ; ) {
            int d, r;
            if ((r = indexRight(q)) != NIL) {
                if (indexesDeletedNode(r)) {
                    unlink(q, r);
                    q = head; // restart
                } else {
                    q = r;
                }
            } else if ((d = indexDown(q)) != NIL) {
                q = d;
            } else {
                for (int b = indexNode(q), n = nodeNext(b); ; ) {
                    if (n == NIL) {
                        return isBaseHeader(b) ? null : b;
                    }
                    int f = nodeNext(n);            // inconsistent read
                    if (n != nodeNext(b)) {
                        break;
                    }
                    long v = nodeValue(n);
                    if (v == NIL) {                 // n is deleted
                        helpDelete(n, b, f);
                        break;
                    }
                    if (nodeValue(b) == NIL || v == SELF) // b is deleted
                    {
                        break;
                    }
                    b = n;
                    n = f;
                }
                q = head; // restart
            }
        }
    }

    /* ---------------- Relational operations -------------- */
    // Control values OR'ed as arguments to findNear
    private static final int EQ = 1;
    private static final int LT = 2;
    private static final int GT = 0; // Actually checked as !LT

    /**
     * Utility for ceiling, floor, lower, higher methods.
     *
     * @param key the key
     * @param rel the relation -- OR'ed combination of EQ, LT, GT
     * @return nearest node fitting relation, or null if no such
     */
    final int findNear(BolBuffer key, int rel) throws InterruptedException {
        if (key == null || key.length == -1) {
            throw new NullPointerException();
        }
        for (; ; ) {

            int b = findPredecessor(key);
            int n = nodeNext(b);
            for (; ; ) {
                long v;
                if (n == NIL) {
                    return ((rel & LT) == 0 || isBaseHeader(b)) ? (int) NIL : b;
                }
                int f = nodeNext(n);
                if (n != nodeNext(b)) // inconsistent read
                {
                    break;
                }
                if ((v = nodeValue(n)) == NIL) {      // n is deleted
                    helpDelete(n, b, f);
                    break;
                }
                if (nodeValue(b) == NIL || v == SELF) // b is deleted
                {
                    break;
                }
                int c = memory.compareBL(key.bytes, key.offset, key.length, nodeKey(n));
                if ((c == 0 && (rel & EQ) != 0)
                    || (c < 0 && (rel & LT) == 0)) {
                    return n;
                }
                if (c <= 0 && (rel & LT) != 0) {
                    return isBaseHeader(b) ? (int) NIL : b;
                }
                b = n;
                n = f;
            }
        }
    }

    private final LABStats stats;
    private final StripingBolBufferLocks bolBufferLocks;
    private final int seed;

    public LABConcurrentSkipListMap(LABStats stats, LABConcurrentSkipListMemory memory, StripingBolBufferLocks bolBufferLocks) throws Exception {
        this.stats = stats;
        this.memory = memory;
        this.bolBufferLocks = bolBufferLocks;
        this.seed = System.identityHashCode(this);
        initialize();
    }

    @Override
    public BolBuffer get(BolBuffer keyBytes, BolBuffer valueBuffer) throws Exception {
        growSemaphore.acquire();
        try {
            long address = doGet(keyBytes);
            if (address == -1) {
                return null;
            } else {
                return memory.acquireBytes(address, valueBuffer);
            }
        } finally {
            growSemaphore.release();
        }
    }

    public int size() throws InterruptedException {
        long count = 0;
        growSemaphore.acquire();
        try {
            for (int n = findFirst(); n != NIL; n = nodeNext(n)) {
                long v = nodeValue(n);
                if (v != NIL && v != SELF) {
                    ++count;
                }
            }
        } finally {
            growSemaphore.release();
        }
        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    }

    @Override
    public boolean isEmpty() throws InterruptedException {
        growSemaphore.acquire();
        try {
            return findFirst() == NIL;
        } finally {
            growSemaphore.release();
        }
    }

    @Override
    public void clear() throws InterruptedException {
        initialize();
    }

    @Override
    public void compute(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBytes,
        BolBuffer valueBuffer,
        LABIndex.Compute<BolBuffer, BolBuffer> remappingFunction,
        LABCostChangeInBytes changeInBytes) throws Exception {

        if (keyBytes == null || keyBytes.length == -1 || remappingFunction == null) {
            throw new NullPointerException();
        }

        synchronized (bolBufferLocks.lock(keyBytes, seed)) {
            growSemaphore.acquire();
            try {
                for (; ; ) {
                    int n;
                    long v;
                    BolBuffer r;
                    if ((n = findNode(keyBytes)) == NIL) {
                        if ((r = remappingFunction.apply(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, null)) == null) {
                            break;
                        }
                        if (!doPut(keyBytes, r, true, changeInBytes)) {
                            return;
                        }
                    } else if ((v = nodeValue(n)) != NIL) {
                        long va = acquireValue(n);
                        if (va != NIL && va != SELF) {
                            long rid;
                            try {
                                BolBuffer acquired = memory.acquireBytes(va, valueBuffer);
                                //memory.release(va);

                                r = remappingFunction.apply(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, acquired);
                                if (r == null) {
                                    rid = NIL;
                                } else {
                                    rid = memory.allocate(r, changeInBytes);
                                    stats.allocationed.add(4 + r.length);
                                }
                            } finally {
                                releaseValue(n);
                            }
                            if (casValue(n, memory, va, rid)) {
                                return;
                            } else if (rid != NIL) {
                                stats.released.add(4 + memory.release(rid));
                            }
                        }
                    }
                }

            } finally {
                growSemaphore.release();
            }
        }
    }

    private final Random random = new Random();

    private boolean doPut(BolBuffer keyBytes, BolBuffer valueBytes, boolean onlyIfAbsent, LABCostChangeInBytes changeInBytes) throws Exception {

        int z;             // added node
        if (keyBytes == null || keyBytes.length == -1) {
            throw new NullPointerException();
        }

        long kid = NIL;
        long vid = NIL;

        outer:
        for (; ; ) {
            int b = findPredecessor(keyBytes);
            int n = nodeNext(b);
            for (; ; ) {
                if (n != NIL) {
                    long v;
                    int c;
                    int f = nodeNext(n);
                    if (n != nodeNext(b)) // inconsistent read
                    {
                        break;
                    }
                    if ((v = nodeValue(n)) == NIL) {   // n is deleted
                        helpDelete(n, b, f);
                        break;
                    }
                    if (nodeValue(b) == NIL || v == SELF) // b is deleted
                    {
                        break;
                    }
                    if ((c = memory.compareBL(keyBytes.bytes, keyBytes.offset, keyBytes.length, nodeKey(n))) > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        if (onlyIfAbsent) {
                            if (kid != NIL) {
                                stats.released.add(4 + memory.release(kid));
                            }
                            if (vid != NIL) {
                                stats.released.add(4 + memory.release(vid));
                            }
                            return true;
                        }
                        if (vid == NIL) {
                            if (valueBytes == null) {
                                vid = NIL;
                            } else {
                                vid = memory.allocate(valueBytes, changeInBytes);
                                stats.allocationed.add(4 + valueBytes.length);
                            }
                        }
                        if (casValue(n, memory, v, vid)) {
                            stats.released.add(4 + memory.release(v));
                            if (kid != NIL) {
                                stats.released.add(4 + memory.release(kid));
                            }
                            if (vid != NIL) {
                                stats.released.add(4 + memory.release(vid));
                            }
                            return true;
                        }
                        break; // restart if lost race to replace value
                    }
                    // else c < 0; fall through
                }

                if (kid == NIL) {
                    kid = memory.allocate(keyBytes, changeInBytes);
                    stats.allocationed.add(4 + keyBytes.length);
                }
                if (vid == NIL) {
                    if (valueBytes == null) {
                        vid = NIL;
                    } else {
                        vid = memory.allocate(valueBytes, changeInBytes);
                        stats.allocationed.add(4 + valueBytes.length);
                    }
                }
                z = allocateNode(kid, vid, n);
                if (!casNext(b, n, z)) {
                    freeNode(z);
                    break;         // restart if lost race to append to b
                } else {
                    kid = NIL;
                    vid = NIL;
                }
                break outer;
            }
        }

        if (kid != NIL) {
            stats.released.add(4 + memory.release(kid));
        }

        if (vid != NIL) {
            stats.released.add(4 + memory.release(vid));
        }

        int rnd = random.nextInt(); // BARF
        if ((rnd & 0x80000001) == 0) { // test highest and lowest bits
            int level = 1, max;
            while (((rnd >>>= 1) & 1) != 0) {
                ++level;
            }
            int idx = (int) NIL;
            int h = head;
            if (level <= (max = indexLevel(h))) {
                for (int i = 1; i <= level; ++i) {
                    idx = allocateIndex(z, idx, (int) NIL, (int) NIL);
                }
            } else { // try to grow by one level
                level = max + 1; // hold in array and later pick the one to use
                @SuppressWarnings("unchecked")
                int[] idxs = new int[level + 1];
                for (int i = 1; i <= level; ++i) {
                    idxs[i] = idx = allocateIndex(z, idx, (int) NIL, (int) NIL);
                }
                for (; ; ) {
                    h = head;
                    int oldLevel = indexLevel(h);
                    if (level <= oldLevel) // lost race to add level
                    {
                        break;
                    }
                    int newh = h;
                    int oldbase = indexNode(h);
                    for (int j = oldLevel + 1; j <= level; ++j) {
                        newh = allocateIndex(oldbase, newh, idxs[j], j);
                    }
                    if (casHead(h, newh)) {
                        h = newh;
                        idx = idxs[level = oldLevel];
                        break;
                    } else {
                        for (int j = oldLevel + 1; j <= level; ++j) {
                            int down = indexDown(newh);
                            freeIndex(newh);
                            newh = down;
                        }
                    }
                }
            }
            // find insertion points and splice in
            splice:
            for (int insertionLevel = level; ; ) {
                int j = indexLevel(h);
                for (int q = h, r = indexRight(q), t = idx; ; ) {
                    if (q == NIL || t == NIL) {
                        break splice;
                    }
                    if (r != NIL) {
                        int n = indexNode(r);
                        // compare before deletion check avoids needing recheck
                        int c = memory.compareBL(keyBytes.bytes, keyBytes.offset, keyBytes.length, nodeKey(n));
                        if (nodeValue(n) == NIL) {
                            if (!unlink(q, r)) {
                                break;
                            }
                            r = indexRight(q);
                            continue;
                        }
                        if (c > 0) {
                            q = r;
                            r = indexRight(r);
                            continue;
                        }
                    }

                    if (j == insertionLevel) {
                        if (!link(q, r, t)) {
                            break; // restart
                        }
                        if (nodeValue(indexNode(t)) == NIL) {
                            findNode(keyBytes);
                            break splice;
                        }
                        if (--insertionLevel == 0) {
                            break splice;
                        }
                    }

                    if (--j >= insertionLevel && j < level) {
                        t = indexDown(t);
                    }
                    q = indexDown(q);
                    r = indexRight(q);
                }
            }
        }
        return false;

    }

    @Override
    public byte[] firstKey() throws InterruptedException {
        growSemaphore.acquire();
        try {
            int n = findFirst();
            if (n == NIL) {
                throw new NoSuchElementException();
            }
            return memory.bytes(nodeKey(n));
        } finally {
            growSemaphore.release();
        }
    }

    @Override
    public byte[] lastKey() throws InterruptedException {
        growSemaphore.acquire();
        try {
            int n = findLast();
            if (n == NIL) {
                throw new NoSuchElementException();
            }
            return memory.bytes(nodeKey(n));
        } finally {
            growSemaphore.release();
        }
    }

    private volatile long addressSign = 1;

    private void compactAll() throws Exception {
        growSemaphore.acquire(ALL);
        try {
            addressSign *= -1;
        } finally {
            growSemaphore.release(ALL);
        }

    }

    public void freeAll() throws Exception {
        growSemaphore.acquire(ALL);
        try {
            for (int n = findFirst(); n != NIL; n = nodeNext(n)) {
                long v = nodeValue(n);
                if (v != NIL && v != SELF) {
                    memory.release((int) nodeKey(n));
                    memory.release((int) nodeValue(n));
                }
            }
        } finally {
            growSemaphore.release(ALL);
        }
        initialize();
    }

    class AllEntryStream implements EntryStream {

        /** the last node returned by next() */
        int lastReturned;
        /** the next node to return from next(); */
        int next;
        /** Cache of next value field to maintain weak consistency */
        long nextValue = NIL;

        /** Initializes ascending iterator for entire range. */
        AllEntryStream() throws InterruptedException {
            while ((next = findFirst()) != NIL) {
                long x = nodeValue(next);
                if (x != NIL && x != SELF) {
                    long va = acquireValue(next);
                    if (va != NIL && va != SELF) {
                        nextValue = va;
                        break;
                    }
                }
            }
        }

        @Override
        public void close() throws InterruptedException {
            if (next != NIL) {
                growSemaphore.acquire();
                try {
                    releaseValue(next);
                } finally {
                    growSemaphore.release();
                }
            }
        }

        @Override
        public final boolean hasNext() {
            return next != NIL;
        }

        @Override
        public boolean next(RawEntryStream entryStream, BolBuffer keyBuffer, BolBuffer valueBuffer) throws Exception {
            BolBuffer acquired = memory.acquireBytes(nextValue, valueBuffer);
            //memory.release(nextValue);

            growSemaphore.acquire();
            try {
                advance();
            } finally {
                growSemaphore.release();
            }
            return entryStream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, acquired);
        }

        void advance() throws InterruptedException {
            if (next == NIL) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            releaseValue(lastReturned);
            while ((next = nodeNext(next)) != NIL) {
                long x = nodeValue(next);
                if (x != NIL && x != SELF) {
                    long va = acquireValue(next);
                    if (va != NIL && va != SELF) {
                        nextValue = va;
                        break;
                    }
                }
            }
        }
    }


    /* ---------------- View Classes -------------- */

    /**
     * Submaps returned by {@link LABConcurrentSkipListMap} submap operations
     * represent a subrange of mappings of their underlying
     * maps. Instances of this class support all methods of their
     * underlying maps, differing in that mappings outside their range are
     * ignored, and attempts to add mappings outside their ranges result
     * in {@link IllegalArgumentException}.  Instances of this class are
     * constructed only using the {@code subMap}, {@code headMap}, and
     * {@code tailMap} methods of their underlying maps.
     *
     * @serial include
     */
    static final class SubRangeEntryStream implements EntryStream {

        /** Underlying map */
        private final LABConcurrentSkipListMap m;
        /** lower bound key, or null if from start */
        private final BolBuffer lo;
        /** upper bound key, or null if to end */
        private final BolBuffer hi;
        /** inclusion flag for lo */
        private final boolean loInclusive;
        /** inclusion flag for hi */
        private final boolean hiInclusive;

        /** the last node returned by next() */
        int lastReturned;
        /** the next node to return from next(); */
        int next;
        /** Cache of next value field to maintain weak consistency */
        long nextValue;

        /**
         * Creates a new submap, initializing all fields.
         */
        SubRangeEntryStream(LABConcurrentSkipListMap map,
            byte[] fromKey, boolean fromInclusive,
            byte[] toKey, boolean toInclusive) throws Exception {

            if (fromKey != null && toKey != null) {
                if (map.memory.compareBB(fromKey, 0, fromKey.length, toKey, 0, toKey.length) > 0) {
                    throw new IllegalArgumentException("inconsistent range");
                }
            }
            this.m = map;
            this.lo = fromKey == null ? null : new BolBuffer(fromKey);
            this.hi = toKey == null ? null : new BolBuffer(toKey);
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;

            BolBuffer keyBolBuffer = new BolBuffer(); // Grr
            map.growSemaphore.acquire();
            try {
                for (; ; ) {
                    next = loNode();
                    if (next == NIL) {
                        break;
                    }
                    long x = m.nodeValue(next);
                    if (x != NIL && x != SELF) {
                        long nk = m.nodeKey(next);
                        BolBuffer acquired = m.memory.acquireBytes(nk, keyBolBuffer);
                        //m.memory.release(nk);
                        if (!inBounds(acquired)) {
                            next = (int) NIL;
                        } else {
                            long va = m.acquireValue(next);
                            if (va != NIL && va != SELF) {
                                nextValue = va;
                            }
                        }
                        break;
                    }
                }
            } finally {
                map.growSemaphore.release();
            }
        }

        @Override
        public void close() throws InterruptedException {
            if (next != NIL) {
                m.growSemaphore.acquire();
                try {
                    m.releaseValue(next);
                } finally {
                    m.growSemaphore.release();
                }
            }
        }

        @Override
        public boolean next(RawEntryStream stream, BolBuffer keyBuffer, BolBuffer valueBuffer) throws Exception {
            BolBuffer acquired = m.memory.acquireBytes(nextValue, valueBuffer);
            //m.memory.release(nextValue);
            m.growSemaphore.acquire();
            try {
                advance(); // Grr
            } finally {
                m.growSemaphore.release();
            }
            return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, acquired);
        }

        @Override
        public boolean hasNext() {
            return next != NIL;
        }

        void advance() throws InterruptedException {
            if (next == NIL) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            m.releaseValue(lastReturned);
            for (; ; ) {
                next = m.nodeNext(next);
                if (next == NIL) {
                    break;
                }
                long x = m.nodeValue(next);
                if (x != NIL && x != SELF) {
                    if (tooHigh(m.nodeKey(next))) {
                        next = (int) NIL;
                    } else {
                        long va = m.acquireValue(next);
                        if (va != NIL && va != SELF) {
                            nextValue = va;
                        }
                    }
                    break;
                }
            }
        }


        /* ----------------  Utilities -------------- */
        boolean tooLow(long key) {
            int c;

            if (lo != null) {
                return ((c = m.memory.compareLB(key, lo.bytes, lo.offset, lo.length)) < 0 || (c == 0 && !loInclusive));

            } else {
                return false;
            }

        }

        boolean tooHigh(long key) {
            int c;
            if (hi != null) {
                return ((c = m.memory.compareLB(key, hi.bytes, hi.offset, hi.length)) > 0 || (c == 0 && !hiInclusive));
            } else {
                return false;
            }

        }

        boolean inBounds(long key) {
            return !tooLow(key) && !tooHigh(key);
        }

        boolean tooLow(BolBuffer key) {
            int c;

            if (lo != null) {
                return ((c = m.memory.compareBB(key.bytes, key.offset, key.length, lo.bytes, lo.offset, lo.length)) < 0 || (c == 0 && !loInclusive));

            } else {
                return false;
            }

        }

        boolean tooHigh(BolBuffer key) {
            int c;
            if (hi != null) {
                return ((c = m.memory.compareBB(key.bytes, key.offset, key.length, hi.bytes, hi.offset, hi.length)) > 0 || (c == 0 && !hiInclusive));
            } else {
                return false;
            }

        }

        boolean inBounds(BolBuffer key) {
            return !tooLow(key) && !tooHigh(key);
        }

        /**
         * Returns true if node key is less than upper bound of range.
         */
        boolean isBeforeEnd(int n) throws InterruptedException {
            if (n == NIL) {
                return false;
            }
            if (hi == null) {
                return true;
            }
            long k = m.nodeKey(n);
            if (k == NIL) // pass by markers and headers
            {
                return true;
            }
            int c = m.memory.compareLB(k, hi.bytes, hi.offset, hi.length);
            if (c > 0 || (c == 0 && !hiInclusive)) {
                return false;
            }
            return true;

        }

        /**
         * Returns lowest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        int loNode() throws InterruptedException {
            if (lo == null) {
                return m.findFirst();
            } else if (loInclusive) {
                return m.findNear(lo, GT | EQ);

            } else {
                return m.findNear(lo, GT);
            }
        }

        /**
         * Returns highest node. This node might not be in range, so
         * most usages need to check bounds.
         */
        int hiNode() throws InterruptedException {
            if (hi == null) {
                return m.findLast();
            } else if (hiInclusive) {
                return m.findNear(hi, LT | EQ);

            } else {
                return m.findNear(hi, LT);

            }
        }

        public int size() throws InterruptedException {

            long count = 0;
            m.growSemaphore.acquire();
            try {
                for (int n = loNode();
                     isBeforeEnd(n);
                     n = m.nodeNext(n)) {
                    int v = (int) m.nodeValue(n);
                    if (v != NIL && v != SELF) {
                        ++count;
                    }
                }
            } finally {
                m.growSemaphore.release();
            }
            return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        }

        public boolean isEmpty() throws InterruptedException {
            m.growSemaphore.acquire();
            try {
                return !isBeforeEnd(loNode());
            } finally {
                m.growSemaphore.release();
            }
        }

    }

    @Override
    public boolean contains(byte[] from, byte[] to) throws Exception {
        growSemaphore.acquire();
        try {
            EntryStream entryStream = rangeMap(from, to);
            try {
                return entryStream.hasNext();
            } finally {
                entryStream.close();
            }
        } finally {
            growSemaphore.release();
        }
    }

    private EntryStream rangeMap(byte[] from, byte[] to) throws Exception {
        if (from != null && to != null) {
            return new SubRangeEntryStream(this, from, true, to, false);
        } else if (from != null) {
            return new SubRangeEntryStream(this, from, true, null, false);
        } else if (to != null) {
            return new SubRangeEntryStream(this, null, false, to, false);
        } else {
            return new AllEntryStream();
        }
    }

    @Override
    public Scanner scanner(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {

        EntryStream entryStream;
        growSemaphore.acquire();
        try {
            entryStream = rangeMap(from != null ? from : null, to != null ? to : null);
        } finally {
            growSemaphore.release();
        }
        BolBuffer keyBuffer = new BolBuffer();
        return new Scanner() {
            @Override
            public Next next(RawEntryStream stream) throws Exception {
                if (entryStream.hasNext()) {
                    boolean more = entryStream.next(stream, keyBuffer, entryBuffer);
                    return more ? Next.more : Next.stopped;
                }
                return Next.eos;
            }

            @Override
            public void close() throws Exception {
                entryStream.close();
            }
        };
    }

    public interface EntryStream {

        boolean hasNext();

        boolean next(RawEntryStream stream, BolBuffer keyBuffer, BolBuffer valueBuffer) throws Exception;

        void close() throws Exception;
    }

    @Override
    public int poweredUpTo() {
        return memory.poweredUpTo();
    }

}
