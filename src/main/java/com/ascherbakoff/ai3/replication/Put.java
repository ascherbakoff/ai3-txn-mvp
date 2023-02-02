package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Put implements Command {
    private UUID txId;
    private final Integer key;
    private final Integer value;

    public Put(Integer key, Integer value, UUID txId) {
        this.txId = txId;
        this.key = key;
        this.value = value;
    }

    public UUID getTxId() {
        return txId;
    }

    public Integer getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }

    @Override
    public void accept(Node node, Request request, CompletableFuture<Response> resp) {
        node.visit(this, request, resp);
    }
}
