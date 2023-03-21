package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.util.BasicTest;
import java.util.ArrayList;
import java.util.List;

public class BasicReplicationTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    Topology top;
    Tracker tracker;
    final NodeId alice = new NodeId("alice");
    NodeId bob = new NodeId("bob");
    NodeId charlie = new NodeId("charlie");
    NodeId leader;

    protected void waitLeaseholder(NodeId nodeId, Tracker tracker, Topology top, String grp) {
        assertEquals(nodeId, tracker.getLeaseHolder(grp));

        Timestamp ts = tracker.clock().get();

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                NodeId leaseHolder = node.getLeaseHolder(grp);
                Timestamp lease = node.getLease(grp);
                if (leaseHolder == null || lease == null)
                    return false;
                return nodeId.equals(leaseHolder) && ts.compareTo(lease) >= 0;
            }, 1_000), "Failed to wait: nodeId=" + node.id());
        }
    }

    protected void createCluster() {
        createCluster(2);
    }

    protected void createCluster(int nodes) {
        top = new Topology();
        top.regiser(new Node(alice, top, clock));
        top.regiser(new Node(bob, top, clock));

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        if (nodes == 3) {
            top.regiser(new Node(charlie, top, clock));
            nodeIds.add(charlie);
        }

        leader = alice;

        tracker = new Tracker(top, clock);
        tracker.register(GRP_NAME, nodeIds);
        tracker.assignLeaseholder(GRP_NAME, leader);

        waitLeaseholder(leader, tracker, top, GRP_NAME);
    }
}
