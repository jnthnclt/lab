package com.github.jnthnclt.os.lab.core.guts;

import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import sun.misc.Unsafe;

/**
 * @author jonathan.colt
 */
public class IndexUtil {

    public static String toString(BolBuffer bb) {
        byte[] copy = bb == null ? null : bb.copy();
        return copy == null ? "NULL" : Arrays.toString(copy);
    }

    /**
     * Borrowed from guava.
     */
    static final boolean BIG_ENDIAN;
    static final Unsafe theUnsafe;
    static final int BYTE_ARRAY_BASE_OFFSET;

    static {
        BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
        theUnsafe = getUnsafe();
        if (theUnsafe != null) {
            BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
            if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                throw new AssertionError();
            }
        } else {
            BYTE_ARRAY_BASE_OFFSET = -1;
        }
    }

    private static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException var2) {
            try {
                return (Unsafe) AccessController.doPrivileged((PrivilegedExceptionAction) () -> {
                    Class k = Unsafe.class;
                    Field[] arr = k.getDeclaredFields();
                    for (Field f : arr) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x)) {
                            return (Unsafe) k.cast(x);
                        }
                    }
                    return null;
                });
            } catch (PrivilegedActionException var1) {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }
    }

    public static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        if (theUnsafe != null) {
            return compareNative(left, leftOffset, leftLength, right, rightOffset, rightLength);
        } else {
            return comparePure(left, leftOffset, leftLength, right, rightOffset, rightLength);
        }
    }

    private static int compareNative(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        int minWords = minLength / 8;

        int i;
        for (i = 0; i < minWords * 8; i += 8) {
            long result = theUnsafe.getLong(left, (long) BYTE_ARRAY_BASE_OFFSET + leftOffset + i);
            long rw = theUnsafe.getLong(right, (long) BYTE_ARRAY_BASE_OFFSET + rightOffset + i);
            if (result != rw) {
                if (BIG_ENDIAN) {
                    return UnsignedLongs.compare(result, rw);
                }

                int n = Long.numberOfTrailingZeros(result ^ rw) & -8;
                return (int) ((result >>> n & 255L) - (rw >>> n & 255L));
            }
        }

        for (i = minWords * 8; i < minLength; ++i) {
            int var11 = UnsignedBytes.compare(left[leftOffset + i], right[rightOffset + i]);
            if (var11 != 0) {
                return var11;
            }
        }

        return leftLength - rightLength;
    }

    private static int comparePure(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left[leftOffset + i] & 0xFF) - (right[rightOffset + i] & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }

    //    public static int compare(IReadable left, int leftLength, BolBuffer right) throws IOException {
////        if (left.canSlice(leftLength)) {
////            return compare(left.slice(leftLength), right);
////        } else {
////            byte[] rawEntry = new byte[leftLength];
////            left.read(rawEntry);
////            return compare(ByteBuffer.wrap(rawEntry), right);
////        }
//
//        int rightLength = right.length;
//        int minLength = Math.min(leftLength, rightLength);
//        for (int i = 0; i < minLength; i++) {
//            int result = (left.read() & 0xFF) - (right.get(i) & 0xFF);
//            if (result != 0) {
//                return result;
//            }
//        }
//        return leftLength - rightLength;
//    }
    public static int compare(BolBuffer left, BolBuffer right) {
        int leftLength = left.length;
        int rightLength = right.length;

//        int minLength = Math.min(leftLength, rightLength);
//        for (int i = 0; i < minLength; i++) {
//            int result = (left.get(i) & 0xFF) - (right.get(i) & 0xFF);
//            if (result != 0) {
//                return result;
//            }
//        }
//        return leftLength - rightLength;
        int minLength = Math.min(leftLength, rightLength);
        int minWords = minLength / 8;

        int i;
        for (i = 0; i < minWords * 8; i += 8) {
            long result = left.getLong(i);
            long rw = right.getLong(i);
            if (result != rw) {
                return UnsignedLongs.compare(result, rw);
            }
        }

        for (i = minWords * 8; i < minLength; ++i) {
            int var11 = UnsignedBytes.compare(left.get(i), right.get(i));
            if (var11 != 0) {
                return var11;
            }
        }

        return leftLength - rightLength;
    }

}
