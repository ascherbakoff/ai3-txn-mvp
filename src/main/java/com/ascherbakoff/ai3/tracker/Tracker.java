package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

public class Tracker {
    public static final int LEASE_DURATION = 20;
    public static final int MAX_CLOCK_SKEW = 5;

    private static System.Logger LOGGER = System.getLogger(Tracker.class.getName());

    private Topology topology;

    private Clock clock = new Clock();

    public Tracker(Topology topology) {
        this.topology = topology;
    }

    private Map<String, Group> groups = new HashMap<>();

    public synchronized void register(String name, List<NodeId> nodeIds) {
        if (groups.containsKey(name))
            return;

        Group group = new Group(name);

        for (NodeId nodeId : nodeIds) {
            group.setState(nodeId, topology.getNode(nodeId) == null ? State.OFFLINE : State.OPERATIONAL);
        }

        groups.put(name, group);
    }

    public void assignLeaseHolders() {
        // TODO
    }

    public Group group(String name) {
        return groups.get(name);
    }

    public void refreshLeaseholder(String name) {
        Group value = groups.get(name);

        if (value == null)
            throw new IllegalArgumentException("Group not found " + name);

        NodeId candidate = null;

        if (value.getLeaseHolder() == null) {
            List<NodeId> nodeIds = new ArrayList<>(value.getNodeState().keySet());

            while (!nodeIds.isEmpty()) {
                int idx = ThreadLocalRandom.current().nextInt(nodeIds.size());

                NodeId nodeId = nodeIds.get(idx);

                Node node = topology.getNode(nodeId);

                if (node == null) {
                    nodeIds.remove(idx);
                    value.setState(nodeId, State.OFFLINE);
                    continue;
                } else if (value.getNodeState().get(nodeId) == State.OFFLINE) {
                    value.setState(nodeId, State.CATCHINGUP); // Node is back again.
                }

                candidate = nodeId; // Found operational node.
                break;
            }
        } else {
            candidate = value.getLeaseHolder();
            // TODO can be offline
        }

        if (candidate == null) {
            LOGGER.log(Level.INFO, "Failed to choose a leaseholder for group, will try again later {0}", value.getName());
            return;
        }

        assert candidate != null;

        Map<NodeId, State> nodeState = value.getNodeState();

        // Check expired lease.
        if (value.getLease() != null) {
            Timestamp expire = value.getLease().adjust(LEASE_DURATION).adjust(MAX_CLOCK_SKEW);

            if (clock.now().compareTo(expire) <= 0) {
                LOGGER.log(Level.INFO, "Failed refresh a leaseholder for group, lease still active {0}", value.getName());
                return;
            }
        }

        Timestamp from = clock.tick();

        LOGGER.log(Level.INFO, "Refreshing a leaseholder: [group={0}, leaseholder={1}, start={2}]", value.getName(), candidate, from);

        for (Entry<NodeId, State> entry : nodeState.entrySet()) {
            if (entry.getValue() == State.OFFLINE)
                continue;

            Node node = topology.getNode(entry.getKey());
            node.refresh(from, candidate, nodeState);
        }
    }

    public enum State {
        OPERATIONAL, OFFLINE, CATCHINGUP
    }
}
