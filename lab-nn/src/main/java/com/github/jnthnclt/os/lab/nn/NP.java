package com.github.jnthnclt.os.lab.nn;

import java.util.concurrent.atomic.AtomicLong;


// NodePoint
public class NP {
    static String[] color = new String[] { "green", "orange", "red", "purple", "blue", "cyan", "gray", "black" };

    public final P n;
    public NP[] nearest = new NP[0];
    public int depth = 0;

    public NP(P n) {
        this.n = n;
    }

    public void dot() {
        for (int i = 0; i < nearest.length; i++) {
            System.out.println(n.id + " -> " + nearest[i].n.id + "[ color=\"" + color[Math.min(i, color.length - 1)] + "\"];");
            nearest[i].dot();
        }

    }

    public NP find(P find, AtomicLong count) {
        count.incrementAndGet();
        double d = Double.MAX_VALUE;
        int displace = -1;
        for (int i = 0; i < nearest.length; i++) {
            count.incrementAndGet();
            double ed = NN.comparableEuclidianDistance(nearest[i].n.features, find.features);
            if (ed < d) {
                d = ed;
                displace = i;
            }
        }
        return displace == -1 ? this : nearest[displace];
    }


}
