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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Group group = (Group) o;

        if (!name.equals(group.name)) {
            return false;
        }
        if (!nodeState.equals(group.nodeState)) {
            return false;
        }
        if (lease != null ? !lease.equals(group.lease) : group.lease != null) {
            return false;
        }
        if (leaseHolder != null ? !leaseHolder.equals(group.leaseHolder) : group.leaseHolder != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + nodeState.hashCode();
        result = 31 * result + (lease != null ? lease.hashCode() : 0);
        result = 31 * result + (leaseHolder != null ? leaseHolder.hashCode() : 0);
        return result;
    }
}
