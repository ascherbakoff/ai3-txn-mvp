package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.util.BasicTest;
import java.util.HashSet;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

public class BasicReplicationTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    Topology top;
    Tracker tracker;
    final NodeId alice = new NodeId("alice");
    NodeId bob = new NodeId("bob");
    NodeId charlie = new NodeId("charlie");
    NodeId leader;
    Set<NodeId> nodeIds;

    protected void waitLeaseholder(Timestamp ts, NodeId nodeId, Tracker tracker, Topology top, String grp) {
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                NodeId leaseHolder = node.getLeaseHolder(grp);
                Timestamp lease = node.getLease(grp);
                if (leaseHolder == null || lease == null)
                    return false;
                return nodeId.equals(leaseHolder) && ts.compareTo(lease) == 0;
            }, 1_000), "Failed to wait for leaseholder: nodeId=" + node.id());
        }
    }

    protected void validateLease(@Nullable NodeId leader) {
        Group grp = null;

        for (NodeId nodeId : nodeIds) {
            Group locGroup = top.getNode(nodeId).group(GRP_NAME);

            if (leader == null) {
                assertNull(top.getNode(nodeId).getLeaseHolder(GRP_NAME));
            } else {
                assertEquals(leader, top.getNode(nodeId).getLeaseHolder(GRP_NAME));
            }

            if (grp == null) {
                grp = locGroup;
            } else {
                assertEquals(grp, locGroup);
            }
        }
    }

    protected void createCluster() {
        createCluster(2);
    }

    protected void createCluster(int nodes) {
        top = new Topology();
        top.regiser(new Node(alice, top, clock));
        top.regiser(new Node(bob, top, clock));

        nodeIds = new HashSet<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        if (nodes == 3) {
            top.regiser(new Node(charlie, top, clock));
            nodeIds.add(charlie);
        }

        leader = alice;

        tracker = new Tracker(top, clock);
        Timestamp ts = tracker.assignLeaseholder(GRP_NAME, leader, nodeIds).join();

        waitLeaseholder(ts, leader, tracker, top, GRP_NAME);
    }
}
