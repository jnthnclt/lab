package com.github.jnthnclt.os.lab.consistency;

/**
 * Created by colt on 11/23/17.
 */
public class Sort {


    public static void sortLI(long[] x, int[] keys, int off, int len) {
        int m;
        int v;
        if (len < 7) {
            for (m = off; m < len + off; ++m) {
                for (v = m; v > off && x[v - 1] > x[v]; --v) {
                    swapLO(x, keys, v, v - 1);
                }
            }

        } else {
            m = off + (len >> 1);
            int a;
            if (len > 7) {
                v = off;
                int n = off + len - 1;
                if (len > 40) {
                    a = len / 8;
                    v = med3L(x, off, off + a, off + 2 * a);
                    m = med3L(x, m - a, m, m + a);
                    n = med3L(x, n - 2 * a, n - a, n);
                }

                m = med3L(x, v, m, n);
            }

            double var13 = (double) x[m];
            a = off;
            int b = off;
            int c = off + len - 1;
            int d = c;

            while (true) {
                while (b > c || (double) x[b] > var13) {
                    for (; c >= b && (double) x[c] >= var13; --c) {
                        if ((double) x[c] == var13) {
                            swapLO(x, keys, c, d--);
                        }
                    }

                    if (b > c) {
                        int n1 = off + len;
                        int s = Math.min(a - off, b - a);
                        vecswapLO(x, keys, off, b - s, s);
                        s = Math.min(d - c, n1 - d - 1);
                        vecswapLO(x, keys, b, n1 - s, s);
                        if ((s = b - a) > 1) {
                            sortLI(x, keys, off, s);
                        }

                        if ((s = d - c) > 1) {
                            sortLI(x, keys, n1 - s, s);
                        }

                        return;
                    }

                    swapLO(x, keys, b++, c--);
                }

                if ((double) x[b] == var13) {
                    swapLO(x, keys, a++, b);
                }

                ++b;
            }
        }
    }

    private static void swapLO(long[] x, int[] keys, int a, int b) {
        long t = x[a];
        x[a] = x[b];
        x[b] = t;
        int l = keys[a];
        keys[a] = keys[b];
        keys[b] = l;
    }

    private static void vecswapLO(long[] x, int[] keys, int a, int b, int n) {
        for (int i = 0; i < n; ++b) {
            swapLO(x, keys, a, b);
            ++i;
            ++a;
        }

    }

    private static int med3L(long[] x, int a, int b, int c) {
        return x[a] < x[b] ? (x[b] < x[c] ? b : (x[a] < x[c] ? c : a)) : (x[b] > x[c] ? b : (x[a] > x[c] ? c : a));
    }
}
