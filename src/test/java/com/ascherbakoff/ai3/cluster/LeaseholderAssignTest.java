package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class LeaseholderAssignTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(LeaseholderAssignTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;

    private void createCluster() { // TODO remove copy paste
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top, clock));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top, clock));

        tracker = new Tracker(top, clock);

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        tracker.register(GRP_NAME, nodeIds);
    }

    @Test
    public void testInitialAssign() {
        createCluster();

        tracker.assignLeaseholder(GRP_NAME, alice);

        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }

        tracker.assignLeaseholder(GRP_NAME, bob);

        // Leaseholders shoudn't change.
        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }
    }

    @Test
    public void testExpire() {
        createCluster();

        tracker.assignLeaseholder(GRP_NAME, alice);

        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }

        adjustClocks(Tracker.LEASE_DURATION);

        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertNull(node.getLeaseHolder(GRP_NAME));
        }

        adjustClocks(Tracker.MAX_CLOCK_SKEW);

        assertNull(tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertNull(node.getLeaseHolder(GRP_NAME));
        }
    }

    @Test
    public void testReassign() {
        createCluster();

        tracker.assignLeaseholder(GRP_NAME, alice);

        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        tracker.assignLeaseholder(GRP_NAME, bob);

        // Leaseholders shoudn't change.
        assertEquals(bob, tracker.getCurrentLeaseHolder(GRP_NAME));

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> bob.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }
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
}
