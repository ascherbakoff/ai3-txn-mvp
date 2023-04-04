package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.NodeId;
import java.util.Set;

public class Configure {
    private final Set<NodeId> members;

    public Configure(Set<NodeId> members) {
        this.members = members;
    }

    public Set<NodeId> getMembers() {
        return members;
    }
}
