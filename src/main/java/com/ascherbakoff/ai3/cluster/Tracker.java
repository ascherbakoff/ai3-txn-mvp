package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Lease;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import com.ascherbakoff.ai3.replication.RpcClient;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;

public class Tracker {
    public static final int LEASE_DURATION = 100;
    public static final int MAX_CLOCK_SKEW = 5;

    private static System.Logger LOGGER = System.getLogger(Tracker.class.getName());

    private Topology topology;

    private final Clock clock;

    public Tracker(Topology topology, Clock clock) {
        this.topology = topology;
        this.client = new RpcClient(topology);
        this.clock = clock;
    }

    private Map<String, Group> groups = new HashMap<>(); // Persistent state - survives restarts.

    private RpcClient client;

    public synchronized void register(String name, List<NodeId> nodeIds) {
        if (groups.containsKey(name))
            return;

        Group group = new Group(name);

        Map<NodeId, State> set = new HashMap<>();

        for (NodeId nodeId : nodeIds) {
            set.put(nodeId, topology.getNode(nodeId) == null ? State.OFFLINE : State.OPERATIONAL);
        }

        Timestamp now = clock.now();
        group.setState(set, now);
        group.commitState(now, true);

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

//    public boolean refreshLeaseholder(String name) {
//        Group group = groups.get(name);
//
//        if (group == null)
//            throw new IllegalArgumentException("Group not found " + name);
//
//        NodeId cur = group.getLeaseHolder();
//
//        NodeId candidate = getLeaseHolder(name);
//
//        if (candidate == null) { // Holder not elected or expired.
//            List<NodeId> nodeIds = new ArrayList<>(group.getNodeState().keySet());
//
//            while (!nodeIds.isEmpty()) {
//                int idx = ThreadLocalRandom.current().nextInt(nodeIds.size());
//
//                NodeId nodeId = nodeIds.get(idx);
//
//                Node node = topology.getNode(nodeId);
//
//                if (node == null) {
//                    nodeIds.remove(idx);
//                    group.setState(nodeId, State.OFFLINE);
//                    continue;
//                } else if (group.getNodeState().get(nodeId) == State.OFFLINE) {
//                    group.setState(nodeId, State.CATCHINGUP); // Node is back again.
//                }
//
//                candidate = nodeId; // Found operational node.
//                break;
//            }
//        } else {
//            candidate = cur; // Try to re-elect previous holder.
//        }
//
//        if (candidate == null) {
//            LOGGER.log(Level.INFO, "Failed to choose a leaseholder for group, will try again later {0}", name);
//            return false;
//        }
//
//        assert candidate != null;
//
//        return assignLeaseholder(name, candidate);
//    }

    /**
     * Assigns (or refreshes) the proposed node to be a leaseholder for the next (or current) lease range.
     *
     * @param name The group name.
     * @param candidate The candidate.
     * @return Assignment future.
     */
    public boolean assignLeaseholder(String name, NodeId candidate) {
        Group group = groups.get(name);

        if (group == null)
            throw new IllegalArgumentException("Group not found " + name);

        assert candidate != null;

        // Check validity.
        NodeId leaseHolder = getLeaseHolder(name);
        if (leaseHolder != null && !leaseHolder.equals(candidate)) {
            LOGGER.log(Level.INFO, "Failed to refresh a leaseholder for group, lease still active [grp={0}, holder={1}, candidate={2}]",
                    group.getName(),
                    group.getLeaseHolder(),
                    candidate);
            return false;
        }

        Timestamp from = clock.now();

        Map<NodeId, State> nodeState = new HashMap<>();

        // Merge group node states with topology state.
        for (NodeId nodeId : group.getNodeState().keySet()) {
            if (topology.getNode(nodeId) == null)
                nodeState.put(nodeId, State.OFFLINE);
            else if (group.getNodeState().get(nodeId) == State.OFFLINE)
                nodeState.put(nodeId, State.CATCHINGUP);
            else
                nodeState.put(nodeId, State.OPERATIONAL);
        }

        if (nodeState.get(candidate) == State.OFFLINE)
            return false; // Can't assign leaseholder on this iteration.

        group.setState(nodeState, from);
        group.commitState(from, true);

        LOGGER.log(Level.INFO, "Assigning(refreshing) a leaseholder: [group={0}, leaseholder={1}, at={2}]", group.getName(), candidate, from);
        group.setLease(from);
        group.setLeaseHolder(candidate);

        NodeId finalCandidate = candidate;
        Request request = new Request();
        request.setGrp(name);
        request.setTs(from);
        request.setPayload(new Lease(name, from, candidate, group.getNodeState()));

        // Send to all alive nodes.
        client.send(candidate, request).thenAccept(new Consumer<Response>() {
            @Override
            public void accept(Response response) {
                for (NodeId nodeId : group.getNodeState().keySet()) {
                    if (nodeId.equals(finalCandidate))
                        continue;

                    if (group.getNodeState().get(nodeId) == State.OFFLINE)
                        continue;

                    client.send(nodeId, request);
                }
            }
        });

        return true;
    }

    public @Nullable NodeId getLeaseHolder(String grpName) {
        Group group = groups.get(grpName);
        if (group == null)
            return null;
        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION).adjust(MAX_CLOCK_SKEW)) < 0)
            return group.getLeaseHolder();

        return null;
    }

    public @Nullable Timestamp getLease(String grpName) {
        Group group = groups.get(grpName);
        if (group == null)
            return null;
        Timestamp now = clock.now();

        Timestamp lease = group.getLease();

        if (lease != null && now.compareTo(lease.adjust(Tracker.LEASE_DURATION).adjust(MAX_CLOCK_SKEW)) < 0)
            return group.getLease();

        return null;
    }

    public enum State {
        OPERATIONAL, OFFLINE, CATCHINGUP
    }
}
