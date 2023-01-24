package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.replication.Response;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class Tracker {
    public static final int LEASE_DURATION = 10;

    private static System.Logger LOGGER = System.getLogger(Tracker.class.getName());

    private Topology topology;

    private Map<NodeId, State> nodeState = new HashMap<>();

    private FD fd = new FD();

    public Tracker(Topology topology) {
        this.topology = topology;
    }

    private Map<Integer, Group> groups = new HashMap<>();

    public void register(Group group) {
        groups.putIfAbsent(group.id(), group);
    }

    public void assignLeaseholders() {
        Timestamp now = Timestamp.now();

        // Iterate over all registered groups (can parallelize)
        for (Integer id : groups.keySet()) {
            assignLeaseholder(id, now);
        }
    }

    public void assignLeaseholder(int id, Timestamp now) {
        Group value = groups.get(id);
        List<NodeId> nodeIds = new ArrayList<>(value.nodeIds());

        Node node0 = null;

        while(!nodeIds.isEmpty()) {
            int idx = ThreadLocalRandom.current().nextInt(nodeIds.size());

            NodeId nodeId = nodeIds.get(idx);

            Node node = topology.getNodeMap().get(nodeId);

            if (node == null) {
                nodeIds.remove(idx);
                continue;
            }

            if (fd.isSuspected(node)) {
                nodeIds.remove(idx);
                continue;
            }

            node0 = node; // Found operational node.
            break;
        }

        if (node0 == null) {
            LOGGER.log(Level.INFO, "Failed to choose a leaseholder for group, will try again later {0}", value.id());
            return;
        }

        assert node0 != null;

        for (NodeId nodeId : nodeIds) {
            Node node = topology.getNodeMap().get(nodeId);
            node.refresh(now, node0, nodeState);
        }
    }

    public enum State {
        OPERATIONAL, OFFLINE, CATCHINGUP
    }
}
