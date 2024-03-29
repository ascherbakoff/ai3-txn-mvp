package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class LeaseProposed implements Command {
    private final String name;
    private final Timestamp from;
    private final Set<NodeId> members;

    public LeaseProposed(String name, Timestamp from, Set<NodeId> members) {
        this.name = name;
        this.from = from;
        this.members = members;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    public Timestamp from() {
        return from;
    }

    /**
     * @return Node state.
     */
    public Set<NodeId> members() {
        return members;
    }
}
