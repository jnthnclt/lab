package org.colt.os.lab;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by jonathan.colt on 4/10/17.
 */
public class LABBoundedExecutor {


    public static ExecutorService newBoundedExecutor(int maxThreads, String name) {
        BlockingQueue<Runnable> queue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable e) {
                return tryTransfer(e);
            }
        };


        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            1,
            maxThreads,
            60,
            TimeUnit.SECONDS,
            queue,
            new ThreadFactoryBuilder().setNameFormat(name + "-%d").build());

        threadPool.setRejectedExecutionHandler((r, executor) -> {
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        return threadPool;
    }

}
