package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Tracker {
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
        for (Group value : groups.values()) {
            // Choose some operational node.
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

            //node0.leaseGranted(now, nodeState);
        }
    }

    public enum State {
        OPERATIONAL, OFFLINE, CATCHINGUP
    }
}
