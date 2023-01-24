package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import org.junit.jupiter.api.Test;

public class TrackerTest {
    @Test
    public void testCreate() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Tracker tracker = new Tracker(t);

        Group group = new Group(0);
        group.addNodeId(alice.id());
        group.addNodeId(bob.id());

        tracker.register(group);

        Timestamp now = Timestamp.now();
        tracker.assignLeaseholder(group.id(), now);
    }

    @Test
    public void testOutOfOrderMessagesIgnored() {

    }
}
