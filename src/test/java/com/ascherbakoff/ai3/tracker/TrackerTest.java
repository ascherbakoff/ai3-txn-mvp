package com.ascherbakoff.ai3.tracker;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrackerTest {
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

        tracker.refreshLeaseholder(name);
    }

    @Test
    public void testOutOfOrderMessagesIgnored() {

    }
}
