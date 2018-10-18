package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ThreadState {

    static private final class BolBuffersLock {
    }

    private AtomicInteger bolBuffersStackDepth = new AtomicInteger();
    private AtomicReferenceArray<BolBuffer> bolBuffers;

    public ThreadState(int bolBufferCapacity) {
        this.bolBuffers = new AtomicReferenceArray<>(new BolBuffer[bolBufferCapacity]);
    }

    public BolBuffer allocate() {
        int i = bolBuffersStackDepth.get();
        if (i > 0 && bolBuffers.get(i - 1) != null) {
            return bolBuffers.getAndSet(bolBuffersStackDepth.decrementAndGet(), null);
        } else {
            return new BolBuffer();
        }
    }

    public void recycle(BolBuffer bolBuffer) {
        if (bolBuffer != null) {
            bolBuffer.reset();
            if (bolBuffersStackDepth.get() < bolBuffers.length()) {
                bolBuffers.set(bolBuffersStackDepth.incrementAndGet(), bolBuffer);
            }
        }
    }
}
