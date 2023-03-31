package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Topology;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

public class RpcClient {
    private static System.Logger LOGGER = System.getLogger(RpcClient.class.getName());

    private Topology topology;

    private @Nullable Predicate<Request> blockPred;

    private List<Object[]> blockedMsgs = new ArrayList<>();

    public RpcClient(Topology topology) {
        this.topology = topology;
    }

    // TODO default timeout, refactor
    public CompletableFuture<Response> send(NodeId nodeId, Request request) {
        CompletableFuture<Response> fut = new CompletableFuture<>();

        Node node = topology.getNodeMap().get(nodeId);

        Objects.requireNonNull(node);

        synchronized (this) {
            if (blockPred != null && blockPred.test(request)) {
                Object[] msgData = {
                        request,
                        System.currentTimeMillis(),
                        (Runnable) () -> node.accept(request).thenAccept(resp -> fut.complete(resp))
                };

                blockedMsgs.add(msgData);

                LOGGER.log(Level.INFO, "Blocked message to={0} id={1} msg={2}", nodeId.toString(), msgData[1].toString(), request);

                return fut;
            }
        }

        return node.accept(request);
    }

    public void unblock(Predicate<Request> pred) {
        ArrayList<Object[]> msgs = new ArrayList<>();

        synchronized (this) {
            Iterator<Object[]> iterator = blockedMsgs.iterator();

            while (iterator.hasNext()) {
                Object[] msg = iterator.next();
                Request r = (Request) msg[0];

                if (pred.test(r)) {
                    msgs.add(msg);
                    iterator.remove();
                }
            }
        }

        for (Object[] msg : msgs) {
            Runnable r = (Runnable) msg[2];

            r.run();
        }
    }

    public void clearBlock() {
        blockPred = null;
    }

    public void block(Predicate<Request> pred) {
        this.blockPred = pred;
    }

    public ArrayList<Request> blocked() {
        ArrayList<Request> msgs = new ArrayList<>();

        synchronized (this) {
            Iterator<Object[]> iterator = blockedMsgs.iterator();

            while (iterator.hasNext()) {
                Object[] msg = iterator.next();

                msgs.add((Request) msg[0]);
            }
        }

        return msgs;
    }
}
