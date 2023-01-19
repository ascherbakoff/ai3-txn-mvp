package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.tracker.NodeId;
import com.ascherbakoff.ai3.tracker.Topology;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class Replicator {
    private NodeId nodeId;

    private Topology topology;

    private SortedMap<Timestamp, Inflight> inflights = new TreeMap<>(); // TODO treeset ?

    private Timestamp lwm = Timestamp.min();

    private RpcClient client;

    public Replicator(NodeId nodeId, Topology topology) {
        this.nodeId = nodeId;
        this.topology = topology;
        this.client = new RpcClient(topology);
    }

    CompletableFuture<Response> send(Object payload) {
        CompletableFuture<Response> fut = new CompletableFuture<>();

        Inflight inflight;
        Timestamp ts;

        synchronized (this) {
            inflights.put((ts = Timestamp.now()), (inflight = new Inflight(ts, fut))); // Must insert in ts order.
        }

        Request request = new Request();
        request.setLwm(ts);
        request.setPayload(payload);

        client.send(nodeId, request).whenComplete(new BiConsumer<Response, Throwable>() {
            @Override
            public void accept(Response response, Throwable throwable) {
                // TODO handle errors.
                synchronized (Replicator.this) {
                    assert throwable == null : throwable;

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
                            entry.getValue().setDone(response); // TODO move out of lock.
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        return fut;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    private static class Inflight {
        private final Timestamp ts;
        private final CompletableFuture<Response> fut;
        private boolean acked;
        private Response done;

        Inflight(Timestamp ts, CompletableFuture<Response> fut) {
            this.ts = ts;
            this.fut = fut;
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
    }


}
