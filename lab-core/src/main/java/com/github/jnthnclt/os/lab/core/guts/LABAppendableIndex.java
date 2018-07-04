package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.AppendEntries;
import com.github.jnthnclt.os.lab.core.guts.api.RawAppendableIndex;
import com.github.jnthnclt.os.lab.core.io.AppendableHeap;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.io.IAppendOnly;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author jonathan.colt
 */
public class LABAppendableIndex implements RawAppendableIndex {

    public static final LABLogger LOG = LABLoggerFactory.getLogger();

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final LongAdder appendedStat;
    private final IndexRangeId indexRangeId;
    private final AppendOnlyFile appendOnlyFile;
    private final int maxLeaps;
    private final int updatesBetweenLeaps;
    private final Rawhide rawhide;
    private final LABHashIndexType hashIndexType;
    private final double hashIndexLoadFactor;
    private final long deleteTombstonedVersionsAfterMillis;

    private LeapFrog latestLeapFrog;
    private int updatesSinceLeap;

    private final long[] startOfEntryIndex;
    private BolBuffer firstKey;
    private BolBuffer lastKey;
    private int leapCount;
    private long count;
    private long keysSizeInBytes;
    private long valuesSizeInBytes;

    private long maxTimestamp = -1;
    private long maxTimestampVersion = -1;

    private volatile IAppendOnly appendOnly;

    public LABAppendableIndex(LongAdder appendedStat,
        IndexRangeId indexRangeId,
        AppendOnlyFile appendOnlyFile,
        int maxLeaps,
        int updatesBetweenLeaps,
        Rawhide rawhide,
        LABHashIndexType hashIndexType,
        double hashIndexLoadFactor,
        long deleteTombstonedVersionsAfterMillis) {

        this.appendedStat = appendedStat;
        this.indexRangeId = indexRangeId;
        this.appendOnlyFile = appendOnlyFile;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.rawhide = rawhide;
        this.hashIndexType = hashIndexType;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.deleteTombstonedVersionsAfterMillis = deleteTombstonedVersionsAfterMillis;

        this.startOfEntryIndex = new long[updatesBetweenLeaps];
    }

    @Override
    public boolean append(AppendEntries appendEntries, BolBuffer keyBuffer) throws Exception {
        if (appendOnly == null) {
            appendOnly = appendOnlyFile.appender();
        }
        // TODO the is be passed in by a provider so we can distort time
        long approximateCurrentVersion = System.currentTimeMillis();
        long deleteIfVersionOldThanTimestamp = approximateCurrentVersion - deleteTombstonedVersionsAfterMillis;

        AtomicLong expiredTombstones = new AtomicLong();
        AppendableHeap appendableHeap = new AppendableHeap(1024);
        appendEntries.consume((rawEntryBuffer) -> {

            //entryBuffer.reset();
            // Unfortunately this impl expect version to me timestampMillis

            long rawEntryTimestamp = rawhide.timestamp(rawEntryBuffer);
            long version = rawhide.version(rawEntryBuffer);

            if (deleteTombstonedVersionsAfterMillis > 0
                && rawhide.tombstone(rawEntryBuffer)
                && version < deleteIfVersionOldThanTimestamp) {
                expiredTombstones.incrementAndGet();
                return true;
            }


            long fp = appendOnly.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + appendableHeap.length();
            appendableHeap.appendByte(ENTRY);

            rawhide.writeRawEntry(rawEntryBuffer, appendableHeap);

            BolBuffer key = rawhide.key(rawEntryBuffer, keyBuffer);
            int keyLength = key.length;
            keysSizeInBytes += keyLength;
            valuesSizeInBytes += rawEntryBuffer.length - keyLength;

            if (rawEntryTimestamp > -1 && maxTimestamp < rawEntryTimestamp) {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = version;
            } else {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = version;
            }

            if (firstKey == null) {
                firstKey = new BolBuffer();
                firstKey.set(key);
            }
            if (lastKey == null) {
                lastKey = new BolBuffer();
            }
            lastKey.set(key);
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendableHeap, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex);
                updatesSinceLeap = 0;
                leapCount++;

                long length = appendableHeap.length();
                appendOnly.append(appendableHeap.leakBytes(), 0, (int) length);
                appendedStat.add(length);
                appendableHeap.reset();
            }
            return true;
        });

        if (appendableHeap.length() > 0) {
            appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
            appendedStat.add(appendableHeap.length());
        }

        if (expiredTombstones.get() > 0) {
            LOG.info("{} records were dropped during the append because there version was more than {} millis old",
                expiredTombstones.get(), deleteTombstonedVersionsAfterMillis);
        }

        return true;
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
        try {

            if (firstKey == null || lastKey == null) {
                throw new IllegalStateException("Tried to close appendable index without a key range: " + this);
            }

            if (appendOnly == null) {
                appendOnly = appendOnlyFile.appender();
            }

            AppendableHeap appendableHeap = new AppendableHeap(8192);
            if (updatesSinceLeap > 0) {
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendableHeap, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex);
                leapCount++;
            }

            appendableHeap.appendByte(FOOTER);
            Footer footer = new Footer(leapCount,
                count,
                keysSizeInBytes,
                valuesSizeInBytes,
                firstKey.copy(),
                lastKey.copy(),
                -1,
                -1,
                maxTimestamp,
                maxTimestampVersion);
            footer.write(appendableHeap);

            appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
            appendedStat.add(appendableHeap.length());
            appendOnly.flush(fsync);
        } finally {
            close();
        }

        buildHashIndex(hashIndexType, count); // HACKY :(
    }


    // TODO this could / should be rewritten to reduce seek thrashing by using batching.
    private void buildHashIndex(LABHashIndexType hashIndexType, long count) throws Exception {
        if (hashIndexLoadFactor > 0) {
            if (hashIndexType == LABHashIndexType.cuckoo) {
                cuckoo(count, false);
            }
            if (hashIndexType == LABHashIndexType.fibCuckoo) {
                cuckoo(count, true);
            }
        }
    }

    private void cuckoo(long count, boolean fib) throws Exception {

        RandomAccessFile f = new RandomAccessFile(appendOnlyFile.getFile(), "rw");
        long length = f.length();

        int chunkPower = UIO.chunkPower(length + 1, 0);
        byte hashIndexLongPrecision = (byte) Math.min((chunkPower / 8) + 1, 8);
        long maxReinsertionsBeforeExtinction = (long) (count * 0.01d);

        byte numHashFunctions;
        PointerReadableByteBufferFile c = null;
        long hashIndexSizeInBytes;


        int extinctions = 0;
        long reinsertion;
        long start = System.currentTimeMillis();
        long clear;
        long[] histo;
        try {
            CUCKOO_EXTINCTION_LEVEL_EVENT:
            while (true) {
                reinsertion = 0;
                if (c != null) {
                    c.close();
                }

                numHashFunctions = (byte) (3 + extinctions);
                long hashIndexMaxCapacity = (count + (long) (count * Math.max(1f, hashIndexLoadFactor))) + 1;
                int twoPower = -1;
                if (fib) {
                    twoPower = UIO.chunkPower(hashIndexMaxCapacity, 1);
                    hashIndexMaxCapacity = 1L << twoPower;
                    twoPower = 63 - twoPower;
                }

                hashIndexSizeInBytes = hashIndexMaxCapacity * hashIndexLongPrecision;
                f.setLength(length + hashIndexSizeInBytes + 1 + 1 + 8 + 4);

                c = new PointerReadableByteBufferFile(ReadOnlyFile.BUFFER_SEGMENT_SIZE, appendOnlyFile.getFile(), true);

                long offset = length;
                for (int i = 0; i < hashIndexMaxCapacity; i++) {
                    c.writeVPLong(offset, 0, hashIndexLongPrecision);
                    offset += hashIndexLongPrecision;
                }
                c.write(offset, numHashFunctions);
                offset++;
                c.write(offset, hashIndexLongPrecision);
                offset++;
                c.writeLong(offset, hashIndexMaxCapacity);
                offset += 8;
                c.writeInt(offset, -2); // cuckoo

                long time = System.currentTimeMillis();
                clear = time - start;
                start = time;


                BolBuffer key = new BolBuffer();
                BolBuffer entryBuffer = new BolBuffer();

                histo = new long[numHashFunctions];
                long inserted = 0;
                long activeOffset = 0;

                while (true) {
                    int type = c.read(activeOffset);
                    activeOffset++;

                    if (type == ENTRY) {
                        long startOfEntryOffset = activeOffset;
                        activeOffset += rawhide.rawEntryToBuffer(c, activeOffset, entryBuffer);

                        BolBuffer k = rawhide.key(entryBuffer, key);


                        inserted++;
                        long displaced = fib ? fibCuckooInsert(numHashFunctions, length, hashIndexLongPrecision, twoPower, c, k, startOfEntryOffset, histo)
                            : cuckooInsert(numHashFunctions, length, hashIndexLongPrecision, hashIndexMaxCapacity, c, k, startOfEntryOffset, histo);

                        long displaceable = inserted;
                        while (displaced != -1) { // cuckoo time
                            reinsertion++;
                            rawhide.rawEntryToBuffer(c, displaced, entryBuffer);
                            k = rawhide.key(entryBuffer, key);
                            displaced = fib ? fibCuckooInsert(numHashFunctions, length, hashIndexLongPrecision, twoPower, c, k, displaced, histo)
                                : cuckooInsert(numHashFunctions, length, hashIndexLongPrecision, hashIndexMaxCapacity, c, k, displaced, histo);
                            displaceable--;
                            if (displaceable < 0 || reinsertion > maxReinsertionsBeforeExtinction) {
                                extinctions++;
                                LOG.debug("Cuckoo: {} with entries:{} capacity:{} numHashFunctions:{} extinctions:{}",
                                    appendOnlyFile.getFile(), count, hashIndexMaxCapacity, numHashFunctions, extinctions);
                                continue CUCKOO_EXTINCTION_LEVEL_EVENT;
                            }
                        }
                    } else if (type == FOOTER) {
                        break;
                    } else if (type == LEAP) {
                        activeOffset += c.readInt(activeOffset);
                    } else {
                        throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
                    }

                }
                f.getFD().sync();
                break;
            }

        } finally {
            if (c != null) {
                c.close();
            }
            f.close();
        }

        LOG.debug(
            "Built hash index for {} with {} entries in {} + {} millis numHashFunctions:{} precision:{} cost:{} bytes reinsertion:{} extinctions:{} histo:{}",
            appendOnlyFile.getFile(),
            count,
            clear,
            System.currentTimeMillis() - start,
            numHashFunctions,
            hashIndexLongPrecision,
            hashIndexSizeInBytes,
            reinsertion,
            extinctions,
            Arrays.toString(histo));

    }
    private long cuckooInsert(byte numHashFunctions,
        long length,
        byte hashIndexLongPrecision,
        long hashIndexMaxCapacity,
        PointerReadableByteBufferFile c,
        BolBuffer k,
        long startOfEntryOffset,
        long[] histo) throws IOException {

        long displaced = -1;
        long hashCode = k.longMurmurHashCode();
        for (int i = 0; i < numHashFunctions; i++) {
            long hi = Math.abs(moduloIndexForHash(hashCode, hashIndexMaxCapacity));
            long pos = length + (hi * hashIndexLongPrecision);
            long v = c.readVPLong(pos, hashIndexLongPrecision);
            if (v == 0) {
                c.writeVPLong(pos, startOfEntryOffset + 1, hashIndexLongPrecision); // +1 so 0 can be null
                break;
            } else if (i + 1 == numHashFunctions) {
                displaced = Math.abs(v) - 1;
                c.writeVPLong(pos, (startOfEntryOffset + 1), hashIndexLongPrecision); // +1 so 0 can be null
            } else if (v > 0) {
                histo[i]++;
                c.writeVPLong(pos, -v, hashIndexLongPrecision);
            }

            hashCode = k.longMurmurHashCode(hashCode);
        }
        return displaced;
    }

    private long fibCuckooInsert(byte numHashFunctions,
        long length,
        byte hashIndexLongPrecision,
        int hashIndexMaxCapacityTwoPower,
        PointerReadableByteBufferFile c,
        BolBuffer k,
        long startOfEntryOffset,
        long[] histo) throws IOException {

        long displaced = -1;
        long hashCode = k.longMurmurHashCode();
        for (int i = 0; i < numHashFunctions; i++) {
            long hi = Math.abs(fibonacciIndexForHash(hashCode, hashIndexMaxCapacityTwoPower));
            long pos = length + (hi * hashIndexLongPrecision);
            long v = c.readVPLong(pos, hashIndexLongPrecision);
            if (v == 0) {
                c.writeVPLong(pos, startOfEntryOffset + 1, hashIndexLongPrecision); // +1 so 0 can be null
                break;
            } else if (i + 1 == numHashFunctions) {
                displaced = Math.abs(v) - 1;
                c.writeVPLong(pos, (startOfEntryOffset + 1), hashIndexLongPrecision); // +1 so 0 can be null
            } else if (v > 0) {
                histo[i]++;
                c.writeVPLong(pos, -v, hashIndexLongPrecision);
            }

            hashCode = k.longMurmurHashCode(hashCode);
        }
        return displaced;
    }

//    public static void main(String[] args)
//    {
//        System.out.printf("%d\n", largestFibonacciLong());
//    }
//
//    public static long largestFibonacciLong()
//    {
//        long temp;
//        long last = 1;
//        long fib = 1;
//
//        while (fib + last > fib) {
//            System.out.println(fib);
//            temp = fib;
//            fib += last;
//            last = temp;
//        }
//
//        return fib;
//    }


    public static long moduloIndexForHash(long hash, long hashIndexMaxCapacity) {
        return hash % hashIndexMaxCapacity;
    }

    public static long fibonacciIndexForHash(long hash, int hashIndexMaxCapacityTwoPower) {
        hash ^= hash >> hashIndexMaxCapacityTwoPower;
        return (7540113804746346429L * hash) >> hashIndexMaxCapacityTwoPower;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(fibonacciIndexForHash(i, 63 - 4));
        }
    }

    public void close() throws IOException {
        appendOnlyFile.close();
        if (appendOnly != null) {
            appendOnly.close();
        }
    }

    public void delete() {
        appendOnlyFile.delete();
    }

    @Override
    public String toString() {
        return "LABAppendableIndex{"
            + "indexRangeId=" + indexRangeId
            + ", index=" + appendOnlyFile
            + ", maxLeaps=" + maxLeaps
            + ", updatesBetweenLeaps=" + updatesBetweenLeaps
            + ", updatesSinceLeap=" + updatesSinceLeap
            + ", leapCount=" + leapCount
            + ", count=" + count
            + '}';
    }

    private LeapFrog writeLeaps(IAppendOnly writeIndex,
        IAppendOnly appendableHeap,
        LeapFrog latest,
        int index,
        BolBuffer key,
        long[] startOfEntryIndex) throws Exception {

        Leaps nextLeaps = LeapFrog.computeNextLeaps(index, key, latest, maxLeaps, startOfEntryIndex);
        appendableHeap.appendByte(LEAP);
        long startOfLeapFp = appendableHeap.getFilePointer() + writeIndex.getFilePointer();
        nextLeaps.write(appendableHeap);
        return new LeapFrog(startOfLeapFp, nextLeaps);
    }


}
