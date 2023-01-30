package com.ascherbakoff.ai3.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrackerTest {
    private static System.Logger LOGGER = System.getLogger(TrackerTest.class.getName());

    @Test
    public void testInitialAssign() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Tracker tracker = new Tracker(t);

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice.id());
        nodeIds.add(bob.id());

        String name = "repl0";
        tracker.register(name, nodeIds);

        tracker.assignLeaseholder(name, alice.id()).join();

        LOGGER.log(Level.INFO, "Tracker clock={0}", tracker.clock().now());

        Group trGroup = tracker.group(name);
        assertEquals(alice.id(), trGroup.getLeaseHolder());

        for (Node value : t.getNodeMap().values()) {
            LOGGER.log(Level.INFO, "Node id={0} clock={1}", value.id(), value.clock().now());
            assertEquals(trGroup, value.group(name));
        }

        tracker.refreshLeaseholder(name).join();

        // Leaseholders shoudn't change.
        for (Node value : t.getNodeMap().values()) {
            LOGGER.log(Level.INFO, "Node id={0} clock={1}", value.id(), value.clock().now());
            assertEquals(trGroup, value.group(name));
        }
    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder.
     * 2. Tracker sends the message but dies before receiving an ack, so leaseholder remains unknown.
     * 3. Tracker is restarted.
     * 4. The tracker attempts to assign a leaseholder after the current lease expires.
     *
     * Expected result: only one leaseholder exist.
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
     * 2. Tracker sends the message to a leaseholder, but the ack is not received due to partition (leasholder node remains alive), so leaseholder remains unknown.
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
