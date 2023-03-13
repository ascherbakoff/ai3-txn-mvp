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
import java.util.concurrent.TimeUnit;

public class Replicator {
    public static final int TIMEOUT_SEC = 1;

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
        CompletableFuture<Response> ioFut = client.send(nodeId, request).orTimeout(TIMEOUT_SEC, TimeUnit.SECONDS);

        Inflight inflight = new Inflight(request.getTs(), ioFut, this);
        inflights.put(inflight.ts, inflight);

        LOGGER.log(Level.DEBUG, "Send id={0}, ts={1}, curLwm={2}", request.getId(), request.getTs(), lwm);

        return inflight;
    }

    public void finish(Inflight inflight) {
        Set<Entry<Timestamp, Inflight>> set = inflights.entrySet();

        Iterator<Entry<Timestamp, Inflight>> iter = set.iterator();

        // Tail cleanup.
        while (iter.hasNext()) {
            Entry<Timestamp, Inflight> entry = iter.next();

            if (entry.getValue().future().isDone() || entry.getValue() == inflight) {
                if (entry.getValue().error()) {
                    break; // Break chain processing.
                }

                iter.remove();
                assert entry.getKey().compareTo(lwm) > 0;
                lwm = entry.getKey(); // Adjust lwm.

                LOGGER.log(Level.DEBUG, "OnRemove id={0}, lwm={1}", inflight.ts, lwm);
            } else {
                break;
            }
        }

        // Adjust local LWM.
        if (nodeId.equals(node.id())) {
            node.group(grp).lwm = lwm;
        }

        // Complete after write.
        inflight.future().complete(null);
    }

    public RpcClient client() {
        return client;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
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
        private final CompletableFuture<Void> fut = new CompletableFuture<>();
        private final CompletableFuture<Response> ioFuture;
        private final Replicator replicator;
        private boolean error = false;

        Inflight(Timestamp now, CompletableFuture<Response> ioFut, Replicator replicator) {
            this.ts = now;
            this.ioFuture = ioFut;
            this.replicator = replicator;
        }

        public CompletableFuture<Void> future() {
            return fut;
        }

        public CompletableFuture<Response> ioFuture() {
            return ioFuture;
        }

        public Timestamp ts() {
            return ts;
        }

        @Override
        public String toString() {
            return "Inflight{" +
                    "ts=" + ts +
                    "err=" + error +
                    ", isDone=" + fut.isDone() +
                    '}';
        }

        public void finish(boolean err) {
            if (fut.isDone())
                return;

            error = err;

            replicator.finish(this);
        }

        public boolean error() {
            return error;
        }
    }
}
