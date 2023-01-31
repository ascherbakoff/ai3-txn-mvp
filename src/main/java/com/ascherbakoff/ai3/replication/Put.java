package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import java.util.concurrent.CompletableFuture;

public class Put implements Command {
    private final Integer key;
    private final Integer value;

    public Put(Integer key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public Integer getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }

    @Override
    public void accept(Node node, CompletableFuture<Response> resp) {
        node.visit(this, resp);
    }
}
