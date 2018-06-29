/*
 * UIO.java
 *
 * Created on 03-12-2010 11:24:38 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jnthnclt.os.lab.core.io.api;

import com.github.jnthnclt.os.lab.core.util.LABLogger;
import com.github.jnthnclt.os.lab.core.util.LABLoggerFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Iterator;

public class UIO {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private static void writeLength(IAppendOnly _filer, int l) throws IOException {
        _filer.appendInt(l);
    }

    public static void writeByteArray(IAppendOnly _filer, byte[] array, String fieldName) throws IOException {
        writeByteArray(_filer, array, 0, array == null ? -1 : array.length, fieldName);
    }

    public static void writeByteArray(IAppendOnly _filer,
        byte[] array,
        int _start,
        int _len,
        String fieldName) throws IOException {

        int len;
        if (array == null) {
            len = -1;
        } else {
            len = _len;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        _filer.append(array, _start, len);
    }

    public static byte[] intBytes(int v) {
        return intBytes(v, new byte[4],0);
    }

    public static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    public static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    public static byte[] shortBytes(short v, byte[] _bytes, int _offset) {
        _bytes[_offset] = (byte) (v >>> 8);
        _bytes[_offset + 1] = (byte) v;
        return _bytes;
    }

    public static short bytesShort(byte[] _bytes) {
        return bytesShort(_bytes, 0);
    }

    public static int[] bytesInts(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int intsCount = _bytes.length / 4;
        int[] ints = new int[intsCount];
        for (int i = 0; i < intsCount; i++) {
            ints[i] = bytesInt(_bytes, i * 4);
        }
        return ints;
    }

    public static int bytesInt(byte[] bytes, int offset) {
        int v = 0;
        v |= (bytes[offset] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 3] & 0xFF);
        return v;
    }

    public static short bytesShort(byte[] bytes, int offset) {
        short v = 0;
        v |= (bytes[offset] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public static int bytesUnsignedShort(byte[] bytes, int offset) {
        int v = 0;
        v |= (bytes[offset] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public static char bytesChar(byte[] bytes, int offset) {
        char v = 0;
        v |= (bytes[offset] & 0xFF);
        v <<= 8;
        v |= (bytes[offset + 1] & 0xFF);
        return v;
    }

    public static byte[] longsBytes(long[] _longs) {
        int len = _longs.length;
        byte[] bytes = new byte[len * 8];
        for (int i = 0; i < len; i++) {
            longBytes(_longs[i], bytes, i * 8);
        }
        return bytes;
    }

    public static byte[] longBytes(long _v) {
        return longBytes(_v, new byte[8], 0);
    }

    public static byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    public static long bytesLong(byte[] _bytes) {
        return bytesLong(_bytes, 0);
    }

    public static long[] bytesLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    public static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    public static int chunkPower(long length, int _minPower) {
        if (length == 0) {
            return 0;
        }
        int numberOfTrailingZeros = Long.numberOfLeadingZeros(length - 1);
        return Math.max(_minPower, 64 - numberOfTrailingZeros);
    }

    public static long chunkLength(int _chunkPower) {
        return 1L << _chunkPower;
    }

    public static int writeBytes(byte[] value, byte[] destination, int offset) {
        if (value != null) {
            System.arraycopy(value, 0, destination, offset, value.length);
            return value.length;
        }
        return 0;
    }

    //Lex key range splittting Copied from HBase
    //Iterate over keys within the passed range.
    public static Iterable<byte[]> iterateOnSplits(byte[] a, byte[] b, boolean inclusive, int num, Comparator<byte[]> lexicographicalComparator) {
        byte[] aPadded;
        byte[] bPadded;
        if (a.length < b.length) {
            aPadded = padTail(a, b.length - a.length);
            bPadded = b;
        } else if (b.length < a.length) {
            aPadded = a;
            bPadded = padTail(b, a.length - b.length);
        } else {
            aPadded = a;
            bPadded = b;
        }
        if (lexicographicalComparator.compare(aPadded, bPadded) >= 0) {
            throw new IllegalArgumentException("b <= a");
        }
        if (num <= 0) {
            throw new IllegalArgumentException("num cannot be <= 0");
        }
        byte[] prependHeader = { 1, 0 };
        BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
        BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
        BigInteger diffBI = stopBI.subtract(startBI);
        if (inclusive) {
            diffBI = diffBI.add(BigInteger.ONE);
        }
        BigInteger splitsBI = BigInteger.valueOf(num + 1);
        //when diffBI < splitBI, use an additional byte to increase diffBI
        if (diffBI.compareTo(splitsBI) < 0) {
            byte[] aPaddedAdditional = new byte[aPadded.length + 1];
            byte[] bPaddedAdditional = new byte[bPadded.length + 1];
            System.arraycopy(aPadded, 0, aPaddedAdditional, 0, aPadded.length);
            System.arraycopy(bPadded, 0, bPaddedAdditional, 0, bPadded.length);
            aPaddedAdditional[aPadded.length] = 0;
            bPaddedAdditional[bPadded.length] = 0;
            return iterateOnSplits(aPaddedAdditional, bPaddedAdditional, inclusive, num, lexicographicalComparator);
        }
        BigInteger intervalBI;
        try {
            intervalBI = diffBI.divide(splitsBI);
        } catch (Exception e) {
            LOG.error("Exception caught during division", e);
            return null;
        }

        Iterator<byte[]> iterator = new Iterator<byte[]>() {
            private int i = -1;

            @Override
            public boolean hasNext() {
                return i < num + 1;
            }

            @Override
            public byte[] next() {
                i++;
                if (i == 0) {
                    return a;
                }
                if (i == num + 1) {
                    return b;
                }

                BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
                byte[] padded = curBI.toByteArray();
                if (padded[1] == 0) {
                    padded = tail(padded, padded.length - 2);
                } else {
                    padded = tail(padded, padded.length - 1);
                }
                return padded;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };

        return () -> iterator;
    }

    /**
     * @param a      array
     * @param length new array size
     * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
     */
    private static byte[] padTail(byte[] a, int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(a, padding);
    }

    /**
     * @param a lower half
     * @param b upper half
     * @return New array that has a in lower half and b in upper half.
     */
    private static byte[] add(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    /**
     * @param a      array
     * @param length amount of bytes to snarf
     * @return Last <code>length</code> bytes from <code>a</code>
     */
    private static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }
}
