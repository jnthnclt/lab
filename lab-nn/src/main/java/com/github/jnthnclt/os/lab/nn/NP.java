package com.github.jnthnclt.os.lab.nn;

import java.util.concurrent.atomic.AtomicLong;


// NodePoint
public class NP {
    static String[] color = new String[] { "green", "orange", "red", "purple", "blue", "cyan", "gray", "black" };

    public final int id;
    public final P p;
    public final int depth;
    public NP[] nearest = new NP[0];

    public NP(int id, P p, int depth) {
        this.id = id;
        this.p = p;
        this.depth = depth;
    }

//    public void dot() {
//        for (int i = 0; i < nearest.length; i++) {
//            System.out.println(n.id + " -> " + nearest[i].n.id + "[ color=\"" + color[Math.min(i, color.length - 1)] + "\"];");
//            nearest[i].dot();
//        }
//
//    }

    public NP find(P find, AtomicLong count, Distance distance) {


        count.incrementAndGet();
        double d = Double.MAX_VALUE;
        int displace = -1;
        for (int i = 0; i < nearest.length; i++) {
            count.incrementAndGet();
            double ed = distance.distance(nearest[i].p.features, find.features);
            if (ed < d) {
                d = ed;
                displace = i;
            }
        }
        return displace == -1 ? this : nearest[displace];

    }

    interface Visitor {
        void visited(P f, int depth, P t);
    }

    public void visit(P f, Visitor visitor) {
        visitor.visited(f, depth, p);
        for (int i = 0; i < nearest.length; i++) {
            nearest[i].visit(p, visitor);
        }
    }

    interface Leafs {
        void leaf(P l);
    }

    public void leafs(Leafs leafs) {

        if (depth == 0) {
            leafs.leaf(p);
        }
        for (int i = 0; i < nearest.length; i++) {
            nearest[i].leafs(leafs);
        }
    }

    interface DepthLeafs {
        void leaf(int depth, P l);
    }

    public void leafForDepth(int d, DepthLeafs leafs) {

        if (depth - 1 == d) {
            leafs.leaf(depth, p);
        }


        if (depth == 0) {
            leafs.leaf(0, p);
        } else {
            for (NP np : nearest) {
                np.leafForDepth(d, leafs);
            }
        }


        if (depth - 1 == d) {
            leafs.leaf(depth, p);
        }
    }

}
