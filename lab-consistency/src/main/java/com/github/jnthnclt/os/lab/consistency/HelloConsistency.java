package com.github.jnthnclt.os.lab.consistency;

public class HelloConsistency {

    public static void main(String[] args) {

        int replication = 3;
        Node[] nodes = new Node[replication];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(i, replication);
        }

        KT e =new KT(0, 0);
        KT got = nodes[0].get(e.timestamp, nodes,0);
        System.out.println("expected null and got:"+got);

        boolean quorum = nodes[0].set( null, e);
        System.out.println("set:"+e+" quorum:"+quorum);


        got = nodes[1].get(e.timestamp, nodes, 0);
        System.out.println("expected 0,0 and got:"+got);


        got = nodes[0].get(e.timestamp, nodes,0);
        System.out.println("expected 0,0 and got:"+got);

        e = new KT(10, 1);
        quorum = nodes[0].set( got, e);
        System.out.println("set:"+e+" quorum:"+quorum);

        got = nodes[1].get(e.timestamp, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        e = new KT(20, 2);
        got = new KT(0, 0);
        quorum = nodes[0].set( e, got);
        System.out.println("try to set in past:"+got+" quorum:"+quorum);

        got = nodes[1].get(2, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        got = nodes[1].get(1, nodes,0);
        System.out.println("expected 1,1 and got:"+got);

        for (Node node : nodes) {
            System.out.println(node);
        }

        for (Node node : nodes) {
            System.out.println(node.get(0,nodes,0));
        }

        for (Node node : nodes) {
            System.out.println(node);
        }

    }
}
