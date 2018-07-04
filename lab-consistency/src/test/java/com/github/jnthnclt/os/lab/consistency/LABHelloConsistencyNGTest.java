package com.github.jnthnclt.os.lab.consistency;

import org.testng.annotations.Test;

public class LABHelloConsistencyNGTest {

    @Test
    public void test() throws Exception {

        ValuesEqual<Long> valuesEqual = (a, b) -> a == b;

        int replication = 3;
        Node<Long>[] nodes = new Node[replication];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node<>(i, replication, valuesEqual);
        }

        Transport transport = new Transport(nodes);
        byte[] key = new byte[]{1};

        ValueTimestamp<Long> e =new ValueTimestamp<>(0L, 0);
        ValueTimestamp<Long> got = nodes[0].get(key, e.timestamp, transport);
        System.out.println("expected null and got:"+got);

        boolean quorum = nodes[0].set(key,  null, e, transport);
        System.out.println("set:"+e+" quorum:"+quorum);


        got = nodes[1].get(key, e.timestamp, transport);
        System.out.println("expected 0,0 and got:"+got);


        got = nodes[0].get(key, e.timestamp, transport);
        System.out.println("expected 0,0 and got:"+got);

        e = new ValueTimestamp<>(10L, 1);
        quorum = nodes[0].set(key,  got, e, transport);
        System.out.println("set:"+e+" quorum:"+quorum);

        got = nodes[1].get(key, e.timestamp, transport);
        System.out.println("expected 1,1 and got:"+got);

        e = new ValueTimestamp<>(20L, 2);
        got = new ValueTimestamp<>(0L, 0);
        quorum = nodes[0].set(key,  e, got, transport);
        System.out.println("try to set in past:"+got+" quorum:"+quorum);

        got = nodes[1].get(key, 2, transport);
        System.out.println("expected 1,1 and got:"+got);

        got = nodes[1].get(key, 1, transport);
        System.out.println("expected 1,1 and got:"+got);

        for (Node<Long> node : nodes) {
            System.out.println(node);
        }

        for (Node<Long> node : nodes) {
            System.out.println(node.get(key, 0,transport));
        }

        for (Node<Long> node : nodes) {
            System.out.println(node);
        }

    }
}
