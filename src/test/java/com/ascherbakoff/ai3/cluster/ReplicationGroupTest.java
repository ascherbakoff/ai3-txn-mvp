package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ReplicationGroupTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(ReplicationGroupTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;

    private void createCluster() {
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top));

        tracker = new Tracker(top);

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        tracker.register(GRP_NAME, nodeIds);
    }

    private void adjustClocks(long delta) {
        tracker.clock().adjust(delta);
        top.getNode(alice).clock().adjust(delta);
        top.getNode(bob).clock().adjust(delta);
    }

    @Test
    public void testBasicReplication() {
        createCluster();

        tracker.assignLeaseholder(GRP_NAME, alice);

        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }

        LOGGER.log(Level.INFO, "Tracker at {0}", tracker.clock().now());
        for (Node node : top.getNodeMap().values()) {
            LOGGER.log(Level.INFO, "Node {0} at {1}", node.id(), node.clock().now());
        }

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();

        for (Node node : top.getNodeMap().values()) {
            assertEquals(0, node.get(0));
        }
    }
}
