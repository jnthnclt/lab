package com.github.jnthnclt.os.lab.consistency;


import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class LABConsistencyNGTest {


    @Test
    public void test() throws Exception {


        int replication = 3;
        Node[] nodes = new Node[replication];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(i, replication);
        }

        Random rand = new Random(1234);
        AtomicLong time = new AtomicLong();

        AtomicReference<KT> max = new AtomicReference<KT>();

        Map<Long, Integer> added = new ConcurrentHashMap<Long, Integer>();

        CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int t = 0; t < 10; t++) {

            new Thread(() -> {

                KT e = new KT(0, 0);
                try {
                    for (int i = 1; i < 100_000; i++) {

                        int id = rand.nextInt(replication);
                        KT g = nodes[id].get(e.timestamp, nodes, 0);
                        if (g != null) {
                            if (g.value < e.value && g.timestamp < e.timestamp) {
                                System.out.println(
                                        Thread.currentThread() + " :( expected:{" + e + "} got:{" + g + "} vd:" + (e.value - g.value) + " td:" + (e.timestamp - g
                                                .timestamp));
                            }

                            if (g.timestamp >= e.timestamp) {
                                added.compute(g.value, (k, v) -> {
                                    if (v == null) {
                                        return 1;
                                    } else {
                                        return v + 1;
                                    }
                                });
                            }

                            e = new KT(g.value + 1, time.incrementAndGet());


                            KT m = max.updateAndGet(kt -> {
                                if (kt == null) {
                                    return g;
                                }

                                if (kt.timestamp > g.timestamp) {
                                    return kt;
                                } else {
                                    return g;
                                }
                            });
                            //System.out.println(Thread.currentThread() + " " + m);
                            id = rand.nextInt(replication);

                            nodes[id].set(g, e);

                            Thread.yield();
                        }
                    }
                } catch (Exception x) {
                    x.printStackTrace();
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }

        countDownLatch.await();
        System.out.println("added:" + added.size());
        for (int i = 1; i < 100_000; i++) {
            if (added.get((long) i) == null) {
                //System.out.println("grrr " + i + " " + added.get((long) i));
            }
        }

    }


}

