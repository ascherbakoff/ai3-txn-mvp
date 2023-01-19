package com.ascherbakoff.ai3.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.tracker.Node;
import com.ascherbakoff.ai3.tracker.NodeId;
import com.ascherbakoff.ai3.tracker.Topology;
import org.junit.jupiter.api.Test;

public class ReplicationTest {
    @Test
    public void testJoin() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        assertEquals(2, t.getNodeMap().size());
    }

    @Test
    public void testSend() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        Response resp0 = aliceToBob.send(new Object()).join();
        assertNotNull(resp0);

        Replicator bobToAlice = new Replicator(alice.id(), t);
        Response resp1 = bobToAlice.send(new Object()).join();
        assertNotNull(resp1);

        assertNotSame(resp0, resp1);
    }

    @Test
    public void testLwmPropagation() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        assertNotNull(aliceToBob.send(new Object()).join());

        Timestamp t0 = aliceToBob.getLwm();
        Timestamp t1 = bob.getLwm();

        assertEquals(t0, t1);

        assertNotNull(aliceToBob.send(new Object()).join());

        Timestamp t2 = aliceToBob.getLwm();
        Timestamp t3 = bob.getLwm();

        assertEquals(t2, t3);
        assertTrue(t2.compareTo(t1) > 0);
    }

    @Test
    public void testLwmPropagationOutOfOrder() {

    }
}
