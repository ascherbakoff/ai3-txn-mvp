package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Topology;
import java.lang.System.Logger.Level;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class Replicator {
    private static System.Logger LOGGER = System.getLogger(Replicator.class.getName());

    private final Node node;

    private final String grp;

    private NodeId nodeId;

    private Topology topology;

    private TreeMap<Timestamp, Inflight> inflights = new TreeMap<>(); // TODO treeset ?

    private Timestamp lwm = Timestamp.min();

    private RpcClient client;

    public Replicator(Node node, NodeId nodeId, String grp, Topology topology) {
        this.node = node;
        this.grp = grp;
        this.nodeId = nodeId;
        this.topology = topology;
        this.client = new RpcClient(topology);
    }

    public Inflight send(Request request) {
        Inflight inflight = new Inflight(request.getTs());
        inflights.put(inflight.ts, inflight);

        LOGGER.log(Level.DEBUG, "Send id={0}, ts={1}, curLwm={2}", request.getId(), request.getTs(), lwm);

        client.send(nodeId, request).whenCompleteAsync(new BiConsumer<Response, Throwable>() {
            @Override
            public void accept(Response response, Throwable throwable) {
                LOGGER.log(Level.DEBUG, "Resp node=" + nodeId + " id=" + request.getId());

                node.clock().onResponse(response.getTs());

                synchronized (Replicator.this) {
                    Set<Entry<Timestamp, Inflight>> set = inflights.entrySet();

                    Iterator<Entry<Timestamp, Inflight>> iter = set.iterator();

                    // Tail cleanup.
                    while (iter.hasNext()) {
                        Entry<Timestamp, Inflight> entry = iter.next();

                        if (entry.getValue().future().isDone() || entry.getValue() == inflight) {
                            iter.remove();
                            assert entry.getKey().compareTo(lwm) > 0;
                            lwm = entry.getKey(); // Adjust lwm.

                            LOGGER.log(Level.DEBUG, "OnRemove id={0}, lwm={1}", request.getTs(), lwm);
                        } else {
                            break;
                        }
                    }

                    // Adjust local LWM.
                    if (nodeId.equals(node.id())) {
                        node.group(grp).lwm = lwm;
                    }

                    // Complete after write.
                    if (throwable == null) {
                        inflight.future().complete(response);
                    } else {
                        inflight.future().completeExceptionally(throwable);
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

    public CompletableFuture<Response> idleSync(Request r) {
        return client.send(nodeId, r).thenApply(response -> {
            node.clock().onResponse(response.getTs());
            return response;
        });
    }

    public int inflights() {
        return inflights.size();
    }

    public Inflight inflight(Timestamp ts) {
        return inflights.get(ts);
    }

    public class Inflight {
        private final Timestamp ts;
        private final CompletableFuture<Response> fut = new CompletableFuture<>();
        private Response done;

        Inflight(Timestamp now) {
            this.ts = now;
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
                    ", isDone=" + fut.isDone() +
                    '}';
        }
    }
}
