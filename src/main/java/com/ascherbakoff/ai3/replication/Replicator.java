package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.tracker.Node;
import com.ascherbakoff.ai3.tracker.NodeId;
import com.ascherbakoff.ai3.tracker.Topology;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

public class Replicator {
    private static System.Logger LOGGER = System.getLogger(Replicator.class.getName());

    private final Node node;

    private NodeId nodeId;

    private Topology topology;

    private TreeMap<Timestamp, Inflight> inflights = new TreeMap<>(); // TODO treeset ?

    private Timestamp lwm = Timestamp.min();

    private RpcClient client;

    public Replicator(Node node, NodeId nodeId, Topology topology) {
        this.node = node;
        this.nodeId = nodeId;
        this.topology = topology;
        this.client = new RpcClient(topology);
    }

    public Inflight send(Command payload) {
        Inflight inflight;

        synchronized (this) {
            inflight = new Inflight();
            inflights.put(inflight.ts, inflight);
            assert inflight == inflights.lastEntry().getValue(); // Must insert in ts order.
        }

        Request request = new Request();
        request.setTs(inflight.ts);
        request.setLwm(lwm);
        request.setPayload(payload);

        LOGGER.log(Level.DEBUG, "Send id={0}, curLwm={1}", request.getTs(), lwm);

        client.send(nodeId, request).whenCompleteAsync(new BiConsumer<Response, Throwable>() {
            @Override
            public void accept(Response response, Throwable throwable) {
                node.clock().onResponse(response.getTs());

                synchronized (Replicator.this) {
                    assert throwable == null : throwable; // TODO handle errors.

                    Set<Entry<Timestamp, Inflight>> set = inflights.entrySet();
                    inflight.setAcked(true);

                    Iterator<Entry<Timestamp, Inflight>> iter = set.iterator();

                    // Tail cleanup.
                    while (iter.hasNext()) {
                        Entry<Timestamp, Inflight> entry = iter.next();

                        if (entry.getValue().isAcked()) {
                            iter.remove();
                            assert entry.getKey().compareTo(lwm) > 0;
                            lwm = entry.getKey(); // Adjust lwm.

                            LOGGER.log(Level.DEBUG, "OnAck id={0}, lwm={1}", request.getTs(), lwm);

                            entry.getValue().setDone(response); // TODO move out of lock.
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        return inflight;
    }

    public RpcClient client() {
        return client;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public CompletableFuture<Response> idleSync() {
        Request r = new Request();
        r.setTs(node.clock().tick()); // Propagate ts in idle sync.
        r.setLwm(lwm);
        return client.send(nodeId, r).thenApply(response -> {
            node.clock().onResponse(response.getTs());
            return response;
        });
    }

    public int inflights() {
        return inflights.size();
    }

    public class Inflight {
        private final Timestamp ts = node.clock().tick();
        private final CompletableFuture<Response> fut = new CompletableFuture<>();
        private boolean acked;
        private Response done;

        Inflight() {
        }

        public void setAcked(boolean acked) {
            this.acked = acked;
        }

        public boolean isAcked() {
            return acked;
        }

        public void setDone(Response done) {
            fut.complete(done);
        }

        public CompletableFuture<Response> future() {
            return fut;
        }

        public Timestamp ts() {
            return ts;
        }

        @Override
        public String toString() {
            return "Inflight{" +
                    "ts=" + ts +
                    ", acked=" + acked +
                    ", isDone=" + fut.isDone() +
                    '}';
        }
    }
}
