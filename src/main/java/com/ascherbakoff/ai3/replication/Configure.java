package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Tracker.State;
import java.util.Map;

public class Configure {
    private final Map<NodeId, State> nodeState;

    public Configure(Map<NodeId, State> nodeState) {
        this.nodeState = nodeState;
    }

    public Map<NodeId, State> getNodeState() {
        return nodeState;
    }
}
