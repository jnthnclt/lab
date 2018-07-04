package com.github.jnthnclt.os.lab.consistency;

import org.testng.annotations.Test;

public class LABHelloConsistencyNGTest {

    @Test
    public void test() throws Exception {

        int replication = 3;
        Node[] nodes = new Node[replication];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(i, replication);
        }
        byte[] key = new byte[]{1};

        KT e =new KT(0, 0);
        KT got = nodes[0].get(key, e.timestamp, nodes,0);
        System.out.println("expected null and got:"+got);

        boolean quorum = nodes[0].set(key,  null, e);
        System.out.println("set:"+e+" quorum:"+quorum);


        got = nodes[1].get(key, e.timestamp, nodes, 0);
        System.out.println("expected 0,0 and got:"+got);


        got = nodes[0].get(key, e.timestamp, nodes,0);
        System.out.println("expected 0,0 and got:"+got);

        e = new KT(10, 1);
        quorum = nodes[0].set(key,  got, e);
        System.out.println("set:"+e+" quorum:"+quorum);

        got = nodes[1].get(key, e.timestamp, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        e = new KT(20, 2);
        got = new KT(0, 0);
        quorum = nodes[0].set(key,  e, got);
        System.out.println("try to set in past:"+got+" quorum:"+quorum);

        got = nodes[1].get(key, 2, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        got = nodes[1].get(key, 1, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        for (Node node : nodes) {
            System.out.println(node);
        }

        for (Node node : nodes) {
            System.out.println(node.get(key, 0,nodes,0));
        }

        for (Node node : nodes) {
            System.out.println(node);
        }

    }
}
