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

        tracker.refreshLeaseholder(name).join();

        LOGGER.log(Level.INFO, "Tracker clock={0}", tracker.clock().now());

        Group trGroup = tracker.group(name);

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

    @Test
    public void testTrackerRestartedBeforeAckReceived() {

    }
}
