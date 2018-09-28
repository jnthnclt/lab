package com.github.jnthnclt.os.lab.base;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class BolBuffers {

    private int bolBuffersStackDepth = 0;
    private AtomicReferenceArray<BolBuffer> bolBuffers;

    public BolBuffers(int capacity) {
        this.bolBuffers = new AtomicReferenceArray<>(new BolBuffer[capacity]);
    }

    public BolBuffer allocate() {

        Thread.currentThread().hashCode();

//        BolBuffer bolBuffer;
//        if (bolBuffersStackDepth > 0 && bolBuffers[bolBuffersStackDepth - 1] != null) {
//            bolBuffersStackDepth--;
//            bolBuffer = bolBuffers[bolBuffersStackDepth];
//            bolBuffers[bolBuffersStackDepth] = null;
//        } else {
//            bolBuffer = new BolBuffer();
//        }
        return new BolBuffer();
    }

    public void recycle(BolBuffer bolBuffer) {
//        if (bolBuffer != null && bolBuffersStackDepth < bolBuffers.length) {
//            bolBuffers[bolBuffersStackDepth] = bolBuffer;
//            bolBuffersStackDepth++;
//        }
    }

    public static long fibonacciIndexForHash(long hash, int hashIndexMaxCapacityTwoPower) {
        hash ^= hash >> hashIndexMaxCapacityTwoPower;
        long index = ((7540113804746346429L * hash) >> hashIndexMaxCapacityTwoPower);
        if (index < 0) {
            return (-index)-1;
        }
        return index;
    }
}
