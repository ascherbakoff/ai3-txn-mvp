package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Collect;
import com.ascherbakoff.ai3.replication.CollectResponse;
import com.ascherbakoff.ai3.replication.Lease;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.RpcClient;
import java.lang.System.Logger.Level;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    public CompletableFuture<Void> assignLeaseholder(String name, NodeId candidate) {
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
            return CompletableFuture.failedFuture(new Exception("Cannot assign lease because previous lease is still active"));
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
            return CompletableFuture.failedFuture(new Exception("Candidate is offline")); // Can't assign leaseholder on this iteration.

        Map<NodeId, Timestamp> lwms = Collections.synchronizedMap(new HashMap<>());

        CompletableFuture<Void> fut = new CompletableFuture<>();

        for (Entry<NodeId, State> entry : nodeState.entrySet()) {
            Request request = new Request();
            request.setGrp(name);
            request.setTs(from);
            request.setPayload(new Collect());

            client.send(entry.getKey(), request).thenAccept(response -> {
                CollectResponse cr = (CollectResponse) response;

                lwms.put(entry.getKey(), cr.getLwm());

                // Collect response from majority.
                if (lwms.size() >= nodeState.size() / 2 + 1) {
                    int oks = 0;

                    for (Timestamp value : lwms.values()) {
                        if (value != null)
                            oks++;
                    }

                    // Fail attempt if can't collect enough lwms.
                    if (oks == nodeState.size() / 2 + 1) {
                        Timestamp ts = Timestamp.min();

                        // Find max.
                        for (Timestamp value : lwms.values()) {
                            if (value != null) {
                                if (value.compareTo(ts) > 0)
                                    ts = value;
                            }
                        }

                        // Candidate must be in the max list, otherwise fail attempt.
                        for (Entry<NodeId, Timestamp> entry0 : lwms.entrySet()) {
                            if (entry0.getKey() == candidate && !entry0.getValue().equals(ts)) {
                                fut.completeExceptionally(new Exception("Cannot assign leaseholder because it is not up to date node")); // TODO codes
                                return;
                            }
                        }

                        LOGGER.log(Level.INFO, "Collected lwms: [group={0}, leaseholder={1}, max={2}]", group.getName(), candidate, ts);

                        group.setState(nodeState, from);
                        group.commitState(from, true);

                        LOGGER.log(Level.INFO, "Assigning a leaseholder: [group={0}, leaseholder={1}, at={2}]", group.getName(), candidate,
                                from);
                        group.setLease(from);
                        group.setLeaseHolder(candidate);

                        fut.complete(null); // TODO this can be optimized by committing(reverting) state only on reply or timeout from candidate.

                        NodeId finalCandidate = candidate;
                        Request request2 = new Request();
                        request2.setGrp(name);
                        request2.setTs(from);
                        request2.setPayload(new Lease(name, from, candidate, group.getNodeState(), ts));

                        // Send to all alive nodes. Next attempt is only possible after current lease expire.
                        client.send(candidate, request2).thenAccept(response2 -> {
                            // TODO attempt can be reverted if a message was definitely not sent.
                            // TODO check errors
                            for (NodeId nodeId : group.getNodeState().keySet()) {
                                if (nodeId.equals(finalCandidate))
                                    continue;

                                if (group.getNodeState().get(nodeId) == State.OFFLINE)
                                    continue;

                                client.send(nodeId, request2);
                            }
                        });
                    } else if (lwms.size() == nodeState.size()) {
                        fut.completeExceptionally(new Exception("Cannot assign a leaseholder because majority not available"));
                    }
                }
            });
        }

        return fut;
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
