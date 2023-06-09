package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
//import com.ascherbakoff.ai3.cluster.Tracker.State;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

public class LeaseGranted implements Command {
    private final String name;
    private final Timestamp from;
    private final NodeId candidate;
    private final Set<NodeId> members;
    private final Timestamp ts;

    public LeaseGranted(String name, Timestamp from, NodeId candidate, Set<NodeId> members, @Nullable Timestamp ts) {
        this.name = name;
        this.from = from;
        this.candidate = candidate;
        this.members = members;
        this.ts = ts;
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
     * @return Candidate.
     */
    public NodeId candidate() {
        return candidate;
    }

    /**
     * @return Node state.
     */
    public Set<NodeId> members() {
        return members;
    }

    public @Nullable Timestamp getTs() {
        return ts;
    }
}
