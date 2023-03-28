package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import com.ascherbakoff.ai3.replication.Configure;
import com.ascherbakoff.ai3.replication.Put;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class LeaseholderAssignTest extends BasicReplicationTest {
    private static System.Logger LOGGER = System.getLogger(LeaseholderAssignTest.class.getName());

    @Test
    public void testInitialAssign() {
        createCluster();

        assertThrows(CompletionException.class, () -> tracker.assignLeaseholder(GRP_NAME, bob).join());

        Group group = tracker.group(GRP_NAME);

        for (NodeId nodeId : group.getNodeState().keySet()) {
            Group locGroup = top.getNode(nodeId).group(GRP_NAME);
            assertEquals(group.nodeState, locGroup.nodeState);
        }

        // Leaseholders shoudn't change.
        assertEquals(alice, tracker.getLeaseHolder(GRP_NAME));
    }

    @Test
    public void testExpire() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION);

        // Tracker lease is still active
        assertEquals(alice, tracker.getLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertNull(node.getLeaseHolder(GRP_NAME));
        }

        adjustClocks(Tracker.MAX_CLOCK_SKEW);

        // Tracker lease expires latest.
        assertNull(tracker.getLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertNull(node.getLeaseHolder(GRP_NAME));
        }
    }

    @Test
    public void testRefresh() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        // Refresh.
        tracker.assignLeaseholder(GRP_NAME, alice).join();
        waitLeaseholder(alice, tracker, top, GRP_NAME);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        // Lease still active after lease duration
        waitLeaseholder(alice, tracker, top, GRP_NAME);
    }

    @Test
    public void testReassign() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        tracker.assignLeaseholder(GRP_NAME, bob);
        waitLeaseholder(bob, tracker, top, GRP_NAME);
    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder.
     * 2. Tracker sends the message but dies before receiving an ack, so leaseholder remains unknown.
     * 3. Tracker is restarted.
     * 4. The tracker attempts to assign a leaseholder after the current lease expires.
     *
     * Expected result: only one leaseholder exists.
     *
     */
    @Test
    public void testScenario1() {

    }

    /**
     * 1. Tracker attempts to assing some node as a leaseholder.
     * 2. Tracker sends the message to a leaseholder, but the message is delayed for t > lease duration.
     * 3. Would-be leaseholder eventually receives the message.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario2() {

    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder
     * 2. Tracker delivers the message to a leaseholder, but the ack is not received due to partition (leasholder node remains alive), so leaseholder remains unknown.
     * 3. Partition condition is healed, but message is lost.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario3() {

    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder
     * 2. Tracker sends the message to a leaseholder, but it was not received due to partition (leasholder node remains alive), so leaseholder remains unknown.
     * 3. Partition condition is healed, but message is lost.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario4() {

    }

    /**
     * Tests replication group size change.
     */
    @Test
    public void testReconfigurationUpscale() {
        createCluster();

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
    }

    @Test
    public void testReconfigurationDownscale() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();

        Group group = tracker.group(GRP_NAME);
        Map<NodeId, State> state = new HashMap<>(group.getNodeState());
        state.put(bob, State.OFFLINE);
        group.setState(state, tracker.clock().now());

        Timestamp ts = leaseholder.replicate(GRP_NAME, new Configure(state)).join();

        Thread.sleep(50);

        for (NodeId nodeId : group.getNodeState().keySet()) {
            Group locGroup = top.getNode(nodeId).group(GRP_NAME);
            assertEquals(group, locGroup, nodeId.toString());
        }
    }
}
