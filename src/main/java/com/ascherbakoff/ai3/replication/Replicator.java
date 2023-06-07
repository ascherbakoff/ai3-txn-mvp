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

/**
 *
 */
public class Replicator {
    public static int TIMEOUT_SEC = 1;

    private static System.Logger LOGGER = System.getLogger(Replicator.class.getName());

    private final Node node;

    private final String grp;

    private NodeId nodeId;

    private Topology topology;

    private long repCntr = 0;

    private Timestamp repTs = Timestamp.min();

    private TreeMap<Long, Inflight> inflights = new TreeMap<>(); // TODO treeset ?

    private RpcClient client;

    private boolean broken;

    public Replicator(Node node, NodeId nodeId, String grp, Topology topology) {
        this.node = node;
        this.grp = grp;
        this.nodeId = nodeId;
        this.topology = topology;
        this.client = new RpcClient(topology);
    }

    public Inflight send(Request request) {
        Replicate payload = (Replicate) request.getPayload(); // TODO FIXME

        if (broken) {
            Inflight inflight = new Inflight(request.getTs(), payload, new CompletableFuture<>());
            inflight.ioFuture().completeExceptionally(new Exception("Broken pipe"));
            return inflight;
        }

        CompletableFuture<Response> ioFut = client.send(nodeId, request).orTimeout(TIMEOUT_SEC, TimeUnit.SECONDS);

        Inflight inflight = new Inflight(request.getTs(), payload, ioFut);
        inflights.put(inflight.getReplicate().getCntr(), inflight);

        LOGGER.log(Level.DEBUG, "Send id={0}, cntr={1}, ts={2}", request.getId(), payload.getCntr(), request.getTs());

        return inflight;
    }

    private void fold() {
        Set<Entry<Long, Inflight>> set = inflights.entrySet();

        Iterator<Entry<Long, Inflight>> iter = set.iterator();

        // Fold consecutive tail.
        while (iter.hasNext()) {
            Entry<Long, Inflight> entry = iter.next();

            if (!entry.getValue().ioFuture().isDone())
                return;

            if (entry.getValue().ioFuture().isCompletedExceptionally()) {
                broken = true;
                return; // TODO replicator is broken.
            }

            if (entry.getKey() > repCntr) {
                return;
            }
            iter.remove();
        }
    }

    public RpcClient client() {
        return client;
    }

    public CompletableFuture<Response> idleSync(Request r) {
        return client.send(nodeId, r).thenApply(response -> {
            System.out.println("idle resp: node=" + node.id() + ", from=" + nodeId + ", ts=" + response.getTs());
            node.clock().onResponse(response.getTs());
            return response;
        });
    }

    public int inflights() {
        return inflights.size();
    }

    public long getRepCntr() {
        return repCntr;
    }

    public Timestamp getRepTs() {
        return repTs;
    }

    public Inflight inflight(Request request) {
        assert request.getPayload() instanceof Replicate;
        Replicate r = (Replicate) request.getPayload();

        return inflights.get(r.getCntr());
    }

    public boolean broken() {
        return broken;
    }

    public void onResponse(ReplicateResponse resp0) {
        if (resp0.getRepCntr() > repCntr) {
            this.repCntr = resp0.getRepCntr();
            this.repTs = resp0.getRepTs();
            fold();
        }
    }

    public void failInflights() {
        for (Inflight value : inflights.values()) {
            value.ioFuture().completeExceptionally(new Exception("Replicator removed"));
        }
    }
}
