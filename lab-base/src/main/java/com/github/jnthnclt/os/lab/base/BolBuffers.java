package com.github.jnthnclt.os.lab.base;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class BolBuffers {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(100);

        BolBuffers bolBuffers = new BolBuffers(2,10);
        List<Future<Void>> futures = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            futures.add(executorService.submit(() -> {
                BolBuffer bolBuffer = bolBuffers.allocate();
                Thread.sleep(1);
                bolBuffers.recycle(bolBuffer);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }
        executorService.shutdownNow();

    }

    static private final class BolBuffersLock {}

    private AtomicInteger bolBuffersStackDepth = new AtomicInteger();
    private AtomicReferenceArray<BolBuffer> bolBuffers;
    private final int hashIndexMaxCapacityTwoPower;
    private final BolBuffersLock[] locks;

    public BolBuffers(int locks, int capacity) {
        this.hashIndexMaxCapacityTwoPower = 63 - UIO.chunkPower(locks, 2);
        this.locks = new BolBuffersLock[1 << UIO.chunkPower(33,2)];
        for (int i = 0; i < this.locks.length; i++) {
            this.locks[i] = new BolBuffersLock();
        }
        this.bolBuffers = new AtomicReferenceArray<>(new BolBuffer[capacity]);
    }

    public BolBuffer allocate() {
        synchronized (locks[(int)fibonacciIndexForHash(Thread.currentThread().hashCode(),hashIndexMaxCapacityTwoPower)]) {
            int i = bolBuffersStackDepth.get();
            if (i > 0 && bolBuffers.get(i - 1) != null) {
                return bolBuffers.getAndSet(bolBuffersStackDepth.decrementAndGet(),null);
            } else {
                return new BolBuffer();
            }
        }
    }

    public void recycle(BolBuffer bolBuffer) {
        if (bolBuffer != null) {
            bolBuffer.reset();
            synchronized (locks[(int)fibonacciIndexForHash(Thread.currentThread().hashCode(), hashIndexMaxCapacityTwoPower)]) {
                if (bolBuffersStackDepth.get() < bolBuffers.length()) {
                    bolBuffers.set(bolBuffersStackDepth.incrementAndGet(),bolBuffer);
                }
            }
        }
    }

    private static long fibonacciIndexForHash(long hash, int hashIndexMaxCapacityTwoPower) {
        hash ^= hash >> hashIndexMaxCapacityTwoPower;
        long index = ((7540113804746346429L * hash) >> hashIndexMaxCapacityTwoPower);
        if (index < 0) {
            return (-index)-1;
        }
        return index;
    }
}
