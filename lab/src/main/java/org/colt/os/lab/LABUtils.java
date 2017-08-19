package org.colt.os.lab;

import org.colt.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABUtils {



    public static int rawArrayLength(byte[] bytes) {
        return 4 + ((bytes == null) ? 0 : bytes.length);
    }

    public static int writeByteArray(byte[] bytes, byte[] destination, int offset) {
        int o = offset;
        if (bytes != null) {
            UIO.intBytes(bytes.length, destination, o);
            o += 4;
            UIO.writeBytes(bytes, destination, o);
            o += bytes.length;
        } else {
            UIO.intBytes(-1, destination, o);
            o += 4;
        }
        return o;
    }

    public static byte[] prefixUpperExclusive(byte[] keyFragment) {
        byte[] upper = new byte[keyFragment.length];
        System.arraycopy(keyFragment, 0, upper, 0, keyFragment.length);

        // given: [64,72,96,0] want: [64,72,97,1]
        // given: [64,72,96,127] want: [64,72,96,-128] because -128 is the next lex value after 127
        // given: [64,72,96,-1] want: [64,72,97,0] because -1 is the lex largest value and we roll to the next digit
        for (int i = upper.length - 1; i >= 0; i--) {
            if (upper[i] == -1) {
                upper[i] = 0;
            } else if (upper[i] == Byte.MAX_VALUE) {
                upper[i] = Byte.MIN_VALUE;
                break;
            } else {
                upper[i]++;
                break;
            }
        }
        return upper;
    }
}
