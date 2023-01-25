package com.ascherbakoff.ai3.tracker;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.tracker.Tracker.State;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class Group {
    private final String name;
    private Map<NodeId, State> nodeState = new HashMap<>();

    private Timestamp lease;

    private @Nullable NodeId leaseHolder;

    public Group(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public @Nullable Timestamp getLease() {
        return lease;
    }

    public void setLease(Timestamp lease) {
        this.lease = lease;
    }

    public NodeId getLeaseHolder() {
        return leaseHolder;
    }

    public void setLeaseHolder(NodeId leaseHolder) {
        this.leaseHolder = leaseHolder;
    }

    public Map<NodeId, State> getNodeState() {
        return nodeState;
    }

    public void setState(NodeId nodeId, State state) {
        nodeState.put(nodeId, state);
    }
}
