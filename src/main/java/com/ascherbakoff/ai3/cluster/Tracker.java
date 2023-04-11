package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Clock;
import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.LeaseProposed;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.RpcClient;
import java.lang.System.Logger.Level;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tracker is stateless.
 */
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

    private RpcClient client;

    public void assignLeaseHolders() {
        // TODO
    }

    public Clock clock() {
        return clock;
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
     * @return Assignment future, containing lease begin on completion.
     */
    public CompletableFuture<Timestamp> assignLeaseholder(String name, NodeId candidate, Set<NodeId> members) {
        Timestamp from = clock.now();

        Request request = new Request();
        request.setGrp(name);
        request.setTs(from);
        request.setPayload(new LeaseProposed(name, from, members));

        return client.send(candidate, request).orTimeout(Replicator.TIMEOUT_SEC, TimeUnit.SECONDS).handle((response, err) -> {
            // TODO handle redirect.
            if (err == null && response.getReturn() != 0) {
                // TODO handle response code. If the candidate responded with error, can immediately try another.
                LOGGER.log(Level.INFO, "Leaseholder rejected: [group={0}, leaseholder={1}, at={2}, reason={3}]", name, candidate, from, response.getMessage());
                throw new RuntimeException(response.getMessage());
            }

            if (err != null) {
                LOGGER.log(Level.INFO, "Leaseholder assigned with error: [group={0}, leaseholder={1}, at={2}, err={3}]", name, candidate, from, err.getMessage());
            } else {
                LOGGER.log(Level.INFO, "Leaseholder assigned: [group={0}, leaseholder={1}, at={2}]", name, candidate, from);
            }

            return from;
        });
    }

    public enum State {
        OPERATIONAL,
        CATCHINGUP,
        IDLE
    }
}
