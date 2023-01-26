package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Response;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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

    public Clock clock() {
        return clock;
    }

    public void assignLeaseHolders() {
        // TODO
    }

    public Group group(String name) {
        return groups.get(name);
    }

    public CompletableFuture<Void> refreshLeaseholder(String name) {
        Group group = groups.get(name);

        if (group == null)
            throw new IllegalArgumentException("Group not found " + name);

        NodeId candidate = null;

        if (group.getLeaseHolder() == null) {
            List<NodeId> nodeIds = new ArrayList<>(group.getNodeState().keySet());

            while (!nodeIds.isEmpty()) {
                int idx = ThreadLocalRandom.current().nextInt(nodeIds.size());

                NodeId nodeId = nodeIds.get(idx);

                Node node = topology.getNode(nodeId);

                if (node == null) {
                    nodeIds.remove(idx);
                    group.setState(nodeId, State.OFFLINE);
                    continue;
                } else if (group.getNodeState().get(nodeId) == State.OFFLINE) {
                    group.setState(nodeId, State.CATCHINGUP); // Node is back again.
                }

                candidate = nodeId; // Found operational node.
                break;
            }
        } else {
            candidate = group.getLeaseHolder();
            // TODO can be offline
        }

        if (candidate == null) {
            LOGGER.log(Level.INFO, "Failed to choose a leaseholder for group, will try again later {0}", group.getName());
            return CompletableFuture.completedFuture(null);
        }

        assert candidate != null;

        Map<NodeId, State> nodeState = group.getNodeState();

        // Check expired lease.
        if (group.getLease() != null) {
            Timestamp expire = group.getLease().adjust(LEASE_DURATION).adjust(MAX_CLOCK_SKEW);

            if (clock.now().compareTo(expire) <= 0) {
                LOGGER.log(Level.INFO, "Failed refresh a leaseholder for group, lease still active [grp={0}]", group.getName());
                return CompletableFuture.completedFuture(null);
            }
        }

        Timestamp from = clock.tick();

        LOGGER.log(Level.INFO, "Refreshing a leaseholder: [group={0}, leaseholder={1}, start={2}]", group.getName(), candidate, from);

        group.setLease(from);

        Node cNode = topology.getNode(candidate);
        if (cNode == null)
            return CompletableFuture.completedFuture(null); // Cant assign leaseholder on this iteration.

        NodeId finalCandidate = candidate;
        return cNode.refresh(name, from, candidate, nodeState).thenAccept(new Consumer<Response>() {
            @Override
            public void accept(Response response) {
                group.setLeaseHolder(finalCandidate);

                for (Entry<NodeId, State> entry : nodeState.entrySet()) {
                    if (entry.getKey().equals(finalCandidate))
                        continue;

                    if (entry.getValue() == State.OFFLINE)
                        continue;

                    Node node = topology.getNode(entry.getKey());
                    node.refresh(name, from, finalCandidate, nodeState);
                }
            }
        });
    }

    public enum State {
        OPERATIONAL, OFFLINE, CATCHINGUP
    }
}
