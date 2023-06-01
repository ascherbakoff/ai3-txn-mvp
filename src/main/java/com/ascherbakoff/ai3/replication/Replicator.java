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
    public static int TIMEOUT_SEC = 10;

    private static System.Logger LOGGER = System.getLogger(Replicator.class.getName());

    private final Node node;

    private final String grp;

    private NodeId nodeId;

    private Topology topology;

    private long repCntr;

    private Timestamp repTs;

    private TreeMap<Long, Inflight> inflights = new TreeMap<>(); // TODO treeset ?

    private RpcClient client;

    private boolean broken;

    public Replicator(Node node, NodeId nodeId, String grp, Topology topology, long cntr) {
        this.node = node;
        this.grp = grp;
        this.nodeId = nodeId;
        this.topology = topology;
        this.client = new RpcClient(topology);
        this.repCntr = cntr;
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

    public void fold() {
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

            if (repCntr + 1 != entry.getKey()) {
                return;
            }

            repTs = entry.getValue().ts();
            repCntr = entry.getKey();
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
}
