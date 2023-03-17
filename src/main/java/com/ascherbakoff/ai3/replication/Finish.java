package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Finish implements Command {
    private final Set<Timestamp> ts;
    private final boolean finish;
    private final Timestamp lwm;

    public Finish(Set<Timestamp> ts, boolean finish, Timestamp lwm) {
        this.ts = ts;
        this.finish = finish;
        this.lwm = lwm;
    }

    public boolean finish() {
        return finish;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }

    public Set<Timestamp> getTs() {
        return ts;
    }
}
