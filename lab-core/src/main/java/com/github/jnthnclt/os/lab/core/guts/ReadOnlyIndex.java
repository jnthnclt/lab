package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.collections.bah.LRUConcurrentBAHLinkedHash;
import com.github.jnthnclt.os.lab.core.api.Snapshot;
import com.github.jnthnclt.os.lab.core.api.exceptions.LABCorruptedException;
import com.github.jnthnclt.os.lab.core.api.rawhide.Rawhide;
import com.github.jnthnclt.os.lab.core.guts.api.ReadIndex;
import com.github.jnthnclt.os.lab.core.guts.api.Scanner;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class ReadOnlyIndex implements ReadIndex {

    private static final AtomicLong CACHE_KEYS = new AtomicLong();
    private final IndexRangeId id;
    private final ReadOnlyFile readOnlyFile;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Footer footer;

    private final Semaphore hideABone;

    private final Rawhide rawhide;
    private LABHashIndexType hashIndexType;
    private byte hashIndexHashFunctionCount = 1;
    private long hashIndexHeadOffset = 0;
    private long hashIndexMaxCapacity = 0;
    private byte hashIndexLongPrecision = 0;
    private Leaps leaps; // loaded when reading

    private final long cacheKey = CACHE_KEYS.incrementAndGet();

    public ReadOnlyIndex(ExecutorService destroy,
        IndexRangeId id,
        ReadOnlyFile readOnlyFile,
        Rawhide rawhide,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.readOnlyFile = readOnlyFile;
        this.hideABone = new Semaphore(Short.MAX_VALUE, true);
        long length = readOnlyFile.length();
        if (length == 0) {
            throw new LABCorruptedException("Trying to construct an index with an empy file.");
        }
        this.footer = readFooter(readOnlyFile.pointerReadable(-1));
        this.rawhide = rawhide;
        this.leapsCache = leapsCache;
    }

    private Footer readFooter(PointerReadableByteBufferFile readable) throws IOException, LABCorruptedException {
        long indexLength = readable.length();
        long seekTo = indexLength - 4;
        seekToBoundsCheck(seekTo, indexLength);
        int footerLength = readable.readInt(seekTo);
        long hashIndexSizeInBytes;
        if (footerLength == -1) { // linearProbe has hash index tacked onto the end.
            seekTo = indexLength - (1 + 8 + 4);
            seekToBoundsCheck(seekTo, indexLength);
            hashIndexLongPrecision = (byte) readable.read(seekTo);
            hashIndexMaxCapacity = readable.readLong(seekTo + 1);
            hashIndexSizeInBytes = (hashIndexMaxCapacity * hashIndexLongPrecision) + 1 + 8 + 4;
            hashIndexHeadOffset = indexLength - hashIndexSizeInBytes;
            seekTo = indexLength - (hashIndexSizeInBytes + 4);
            seekToBoundsCheck(seekTo, indexLength);
            footerLength = readable.readInt(seekTo);
            seekTo = indexLength - (hashIndexSizeInBytes + 1 + footerLength);
            hashIndexType = LABHashIndexType.linearProbe;
        } else if (footerLength == -2) { // cuckoo hash index tacked onto the end.
            seekTo = indexLength - (1 + 1 + 8 + 4);
            seekToBoundsCheck(seekTo, indexLength);
            hashIndexHashFunctionCount = (byte) readable.read(seekTo);
            hashIndexLongPrecision = (byte) readable.read(seekTo + 1);
            hashIndexMaxCapacity = readable.readLong(seekTo + 1 + 1);
            hashIndexSizeInBytes = (hashIndexMaxCapacity * hashIndexLongPrecision) + 1 + 1 + 8 + 4;
            hashIndexHeadOffset = indexLength - hashIndexSizeInBytes;
            seekTo = indexLength - (hashIndexSizeInBytes + 4);
            seekToBoundsCheck(seekTo, indexLength);
            footerLength = readable.readInt(seekTo);
            seekTo = indexLength - (hashIndexSizeInBytes + 1 + footerLength);
            hashIndexType = LABHashIndexType.cuckoo;
        } else {
            seekTo = indexLength - (1 + footerLength);
        }

        seekToBoundsCheck(seekTo, indexLength);
        int type = readable.read(seekTo);
        seekTo++;
        if (type != LABAppendableIndex.FOOTER) {
            throw new LABCorruptedException(
                "Footer Corruption! Found " + type + " expected " + LABAppendableIndex.FOOTER + " within file:" + readOnlyFile.getFileName() + " length:" +
                    readOnlyFile
                    .length());
        }
        return Footer.read(readable, seekTo);
    }

    private void seekToBoundsCheck(long seekTo, long indexLength) throws IOException, LABCorruptedException {
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABCorruptedException(
                "Corruption! trying to seek to: " + seekTo + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length());
        }
    }


    public String name() {
        return id + " " + readOnlyFile.getFileName();
    }

    public IndexRangeId id() {
        return id;
    }

    // you must call release on this reader! Try to only use it as long have you have to!
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get() || readOnlyFile.isClosed()) {
            hideABone.release();
            return null;
        }

        try {
            if (leaps == null) {
                PointerReadableByteBufferFile readableIndex = readOnlyFile.pointerReadable(-1);
                long indexLength = readableIndex.length();

                long seekTo = indexLength - 4;
                seekToBoundsCheck(seekTo, indexLength);
                int footerLength = readableIndex.readInt(seekTo);
                long hashIndexSizeInBytes = 0;
                if (footerLength == -1) { // linearProbe hash index tacked onto the end.
                    seekTo = indexLength - (1 + 8 + 4);
                    seekToBoundsCheck(seekTo, indexLength);
                    byte hashIndexLongPrecision = (byte) readableIndex.read(seekTo);
                    long hashIndexMaxCapacity = readableIndex.readLong(seekTo + 1);
                    hashIndexSizeInBytes = (hashIndexMaxCapacity * hashIndexLongPrecision) + 1 + 8 + 4;
                    seekTo = indexLength - (hashIndexSizeInBytes + 4);
                    seekToBoundsCheck(seekTo, indexLength);
                    footerLength = readableIndex.readInt(seekTo);
                    seekTo = indexLength - (hashIndexSizeInBytes + footerLength + 1 + 4);
                } else if (footerLength == -2) { // cuckoo hash index tacked onto the end.
                    seekTo = indexLength - (1 + 1 + 8 + 4);
                    seekToBoundsCheck(seekTo, indexLength);
                    //byte hashIndexHashFunctionCount = (byte) readableIndex.read(seekTo);
                    byte hashIndexLongPrecision = (byte) readableIndex.read(seekTo + 1);
                    long hashIndexMaxCapacity = readableIndex.readLong(seekTo + 1 + 1);
                    hashIndexSizeInBytes = (hashIndexMaxCapacity * hashIndexLongPrecision) + 1 + 1 + 8 + 4;
                    seekTo = indexLength - (hashIndexSizeInBytes + 4);
                    seekToBoundsCheck(seekTo, indexLength);
                    footerLength = readableIndex.readInt(seekTo);
                    seekTo = indexLength - (hashIndexSizeInBytes + footerLength + 1 + 4);
                } else {
                    seekTo = indexLength - (footerLength + 1 + 4);
                }
                seekToBoundsCheck(seekTo, indexLength);
                int leapLength = readableIndex.readInt(seekTo);

                seekTo = indexLength - (hashIndexSizeInBytes + 1 + leapLength + 1 + footerLength);
                seekToBoundsCheck(seekTo, indexLength);
                seekTo = indexLength - (hashIndexSizeInBytes + 1 + leapLength + 1 + footerLength);

                int type = readableIndex.read(seekTo);
                seekTo++;
                if (type != LABAppendableIndex.LEAP) {
                    throw new LABCorruptedException(
                        "4. Leaps Corruption! " + type + " expected " + LABAppendableIndex.LEAP + " file:" + readOnlyFile.getFileName() + " length:" +
                            readOnlyFile.length()
                    );
                }
                leaps = Leaps.read(readableIndex, seekTo);
            }


            return this;
        } catch (IOException | RuntimeException x) {
            hideABone.release();
            throw x;
        }
    }


    public void snapshot(Snapshot snapshot) throws Exception {
        hideABone.acquire();
        if (disposed.get() || readOnlyFile.isClosed()) {
            hideABone.release();
            return;
        }

        try {
            snapshot.file(readOnlyFile.getFile());
        } catch (Exception x) {
            hideABone.release();
            throw x;
        }
    }


    public void destroy() throws Exception {
        if (destroy != null) {
            destroy.submit(() -> {

                hideABone.acquire(Short.MAX_VALUE);
                disposed.set(true);
                try {
                    readOnlyFile.close();
                    readOnlyFile.delete();
                } finally {
                    hideABone.release(Short.MAX_VALUE);
                }
                return null;
            });
        } else {
            throw new UnsupportedOperationException("This was constructed such that destroy isn't supported");
        }
    }

    public void fsync() throws Exception {
        hideABone.acquire();
        try {
            if (!disposed.get() && !readOnlyFile.isClosed()) {
                readOnlyFile.fsync();
            }
        } finally {
            hideABone.release();
        }
    }

    public void closeReadable() throws Exception {
        hideABone.acquire(Short.MAX_VALUE);
        try {
            readOnlyFile.close();
        } finally {
            hideABone.release(Short.MAX_VALUE);
        }
    }

    private ActiveScanRow setup(ActiveScanRow activeScan) throws IOException {
        activeScan.rawhide = rawhide;
        activeScan.leaps[0] = leaps;
        activeScan.cacheKey = cacheKey;
        activeScan.leapsCache = leapsCache;
        activeScan.cacheKeyBuffer = new byte[16];
        activeScan.readable = readOnlyFile.pointerReadable(-1);
        return activeScan;
    }

    private ActiveScanRange setup(ActiveScanRange activeScan) throws IOException {
        activeScan.rawhide = rawhide;
        activeScan.leaps = leaps;
        activeScan.cacheKey = cacheKey;
        activeScan.leapsCache = leapsCache;
        activeScan.readable = readOnlyFile.pointerReadable(-1);
        activeScan.cacheKeyBuffer = new byte[16];

        activeScan.hashIndexType = hashIndexType;
        activeScan.hashIndexHashFunctionCount = hashIndexHashFunctionCount;
        activeScan.hashIndexHeadOffset = hashIndexHeadOffset;
        activeScan.hashIndexMaxCapacity = hashIndexMaxCapacity;
        activeScan.hashIndexLongPrecision = hashIndexLongPrecision;

        activeScan.activeOffset = -1;
        return activeScan;
    }


    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public Scanner rangeScan(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {

        BolBuffer bbFrom = from == null ? null : new BolBuffer(from);
        BolBuffer bbTo = to == null ? null : new BolBuffer(to);

        ActiveScanRange scan = setup(new ActiveScanRange());
        long fp = scan.getInclusiveStartOfRow(new BolBuffer(from), entryBuffer, entryKeyBuffer, false);
        if (fp < 0) {
            return null;
        }
        scan.setupAsRangeScanner(fp, to, entryBuffer, entryKeyBuffer, bbFrom, bbTo);
        return scan;
    }

    @Override
    public Scanner rowScan(BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
        return setup(new ActiveScanRow());
    }

    @Override
    public BolBuffer pointScan(boolean hashIndexEnabled, byte[] key) throws Exception {

        BolBuffer entryBuffer = new BolBuffer();

        byte[] cacheKeyBuffer = new byte[16];
        PointerReadableByteBufferFile readable = readOnlyFile.pointerReadable(-1);
        long fp = ActiveScan.getInclusiveStartOfRow(readable,
            leaps,
            cacheKey,
            leapsCache,
            cacheKeyBuffer,
            rawhide,
            hashIndexEnabled,
            hashIndexType,
            hashIndexHashFunctionCount,
            hashIndexHeadOffset,
            hashIndexMaxCapacity,
            hashIndexLongPrecision,
            new BolBuffer(key),
            new BolBuffer(),
            new BolBuffer(),
            true);

        if (fp < 0) {
            return null;
        }
            int type;
            while ((type = readable.read(fp)) >= 0) {
                fp++;
                if (type == LABAppendableIndex.ENTRY) {
                    fp += rawhide.rawEntryToBuffer(readable, fp, entryBuffer);
                    return entryBuffer;
                } else if (type == LABAppendableIndex.LEAP) {
                    int length = readable.readInt(fp); // entryLength
                    fp += (length);
                } else if (type == LABAppendableIndex.FOOTER) {
                    return null;
                } else {
                    throw new IllegalStateException("Bad row type:" + type + " at fp:" + (fp - 1));
                }
            }
            throw new IllegalStateException("Missing footer");


//
//        ActiveScanPoint pointScan = setup(new ActiveScanPoint(hashIndexEnabled));
//        long fp = pointScan.getInclusiveStartOfRow(new BolBuffer(key), new BolBuffer(), new BolBuffer(), true);
//        if (fp < 0) {
//            return null;
//        }
//        pointScan.setupPointScan(fp);
//        return pointScan;
    }

    @Override
    public long count() throws IOException {
        return footer.count;
    }

    public long sizeInBytes() throws IOException {
        return readOnlyFile.length();
    }

    public long keysSizeInBytes() throws IOException {
        return footer.keysSizeInBytes;
    }

    public long valuesSizeInBytes() throws IOException {
        return footer.valuesSizeInBytes;
    }

    public byte[] minKey() {
        return footer.minKey;
    }

    public byte[] maxKey() {
        return footer.maxKey;
    }

    public Footer footer() {
        return footer;
    }

    @Override
    public String toString() {
        return "LeapsAndBoundsIndex{" + "id=" + id + ", index=" + readOnlyFile + ", disposed=" + disposed + ", footer=" + footer + '}';
    }

}
