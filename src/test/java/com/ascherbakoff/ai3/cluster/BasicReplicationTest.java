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

    NodeId dave = new NodeId("dave");
    NodeId eve = new NodeId("eve");

    Set<NodeId> nodeIds;

    protected void waitLeader(Timestamp ts, NodeId nodeId, Tracker tracker, Topology top, String grp) {
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                NodeId leader = node.getLeader(grp);
                Timestamp lease = node.getLease(grp);
                if (leader == null || lease == null)
                    return false;
                return nodeId.equals(leader) && ts.equals(lease);
            }, 1_000), "Failed to wait for leader: nodeId=" + node.id());
        }
    }

    protected void waitReplication() {
        Node leader = top.getNode(alice);
        Timestamp repTs = leader.group(GRP_NAME).getRepTs();
        long repCntr = leader.group(GRP_NAME).getRepCntr();

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                return top.getNode(node.id()).group(GRP_NAME).getRepTs().equals(repTs);
            }, 1_000), "Failed to wait for repTs: nodeId=" + node.id() + ", exp=" + repTs + ", actual=" + top.getNode(node.id()).group(GRP_NAME).getRepTs());
        }

        assertTrue(waitForCondition(() -> {
            Timestamp safeTs = leader.group(GRP_NAME).getSafeTs();
            return safeTs.equals(repTs);
        }, 1_000), "Failed to wait for safeTs: exp=" + repTs + ", actual=" + leader.group(GRP_NAME).getSafeTs());

        assertEquals(repCntr, leader.group(GRP_NAME).getSafeCntr());
    }

    protected void validate(@Nullable NodeId leader, NodeId... exclude) {
        Group grp = null;

        for (Node node : top.getNodeMap().values()) {
            boolean skip = false;
            for (NodeId nodeId : exclude) {
                if (node.id().equals(nodeId)) {
                    skip = true;
                }
            }

            if (skip)
                continue;

            Group locGroup = node.group(GRP_NAME);

            if (leader == null) {
                assertNull(node.getLeader(GRP_NAME));
            } else {
                assertEquals(leader, node.getLeader(GRP_NAME));
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
        top.regiser(new Node(alice, top, clock, GRP_NAME));
        top.regiser(new Node(bob, top, clock, GRP_NAME));

        nodeIds = new HashSet<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        if (nodes >= 3) {
            top.regiser(new Node(charlie, top, clock, GRP_NAME));
            nodeIds.add(charlie);
        }

        if (nodes >= 5) {
            top.regiser(new Node(dave, top, clock, GRP_NAME));
            nodeIds.add(dave);

            top.regiser(new Node(eve, top, clock, GRP_NAME));
            nodeIds.add(eve);
        }

        tracker = new Tracker(top, clock);
        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, nodeIds).join();

        waitLeader(ts, alice, tracker, top, GRP_NAME);
    }
}
