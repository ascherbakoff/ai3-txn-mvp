package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Inflight;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Local replication group.
 */
public class Group {
    // Group name.
    private final String name;

    // Replication update counter.
    private long repCntr;

    // Replication timestamp.
    private Timestamp repTs = Timestamp.min();

    // Replicators for this group on a leader.
    Map<NodeId, Replicator> replicators = new HashMap<>();

    // Replica inflights (used by replica).
    private TreeMap<Long, Inflight> repInflights = new TreeMap<Long, Inflight>();

    // Snap index. TODO compaction.
    TreeMap<Timestamp, Replicate> snapIdx = new TreeMap<Timestamp, Replicate>();

    // Maintained on a leader.
    // TODO rename safe <-> rep
    private long safeCntr;
    private Timestamp safeTs;

    // Group state.
    private @Nullable Timestamp lease;
    private @Nullable NodeId leader;
    private Set<NodeId> members = Collections.emptySet();

    // Read requests, waiting for repTs.
    public TreeMap<Timestamp, Read> pendingReads = new TreeMap<>();

    public Group(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public @Nullable Timestamp getLease() {
        return lease;
    }

    public long getRepCntr() {
        return repCntr;
    }

    public void setLease(Timestamp lease) {
        this.lease = lease;
    }

    public NodeId getLeader() {
        return leader;
    }

    public void setLeader(NodeId leader) {
        this.leader = leader;
    }

    public Set<NodeId> getMembers() {
        return members;
    }

    public void setState(Set<NodeId> members) {
        this.members = new HashSet<>(members);
    }

    public ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(name + "-sender-thread");
            return thread;
        }
    });

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Group group = (Group) o;

        if (!name.equals(group.name)) {
            return false;
        }
        if (Long.compare(repCntr, group.repCntr) != 0) {
            return false;
        }
        if (repTs != null ? !repTs.equals(group.repTs) : group.repTs != null) {
            return false;
        }
        if (members != null ? !members.equals(group.members) : group.members != null) {
            return false;
        }
        if (lease != null ? !lease.equals(group.lease) : group.lease != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public boolean validLease(Timestamp at, NodeId leaseHolder) {
        if (!leaseHolder.equals(this.leader)) // Take into account the case of no leaseholder at all.
        {
            return false;
        }

        assert lease != null;

        return lease.compareTo(at) <= 0 && at.compareTo(lease.adjust(Tracker.LEASE_DURATION)) < 0 ? true : false;
    }

    public long nextCounter() {
        return ++repCntr;
    }

    public void accept(Timestamp repTs, Replicate replicate, boolean local) {
        if (!local && this.repTs.compareTo(repTs) > 0)
            return; // Already replicated by catch up.

        // Track out of order updates on replica.
        Inflight inflight = new Inflight(repTs, replicate, null);

        snapIdx.put(repTs, replicate);

        if (local) {
            setRepTs(repTs); // Counter is already updated in the caller.
            return;
        }

        repInflights.put(replicate.getCntr(), inflight);

        Set<Entry<Long, Inflight>> set = repInflights.entrySet();

        Iterator<Entry<Long, Inflight>> iter = set.iterator();

        // Fold consecutive tail.
        while (iter.hasNext()) {
            Entry<Long, Inflight> entry = iter.next();

            if (repCntr + 1 == entry.getKey()) {
                setRepTs(entry.getValue().ts());
                iter.remove();
                repCntr++;
            }
        }
    }

    public void setIdle(Timestamp ts) {
        assert repInflights.isEmpty();

        if (ts.compareTo(this.repTs) > 0) {
            this.repTs = ts;
        }
    }

    public Timestamp getRepTs() {
        return repTs;
    }

    // safeTs is updated on stable topology.
    public void updateSafe() {
        int maj = majority();

        if (maj == 1) {
            this.safeCntr = repCntr;
            this.safeTs = repTs;
        } else {
            // Retain only stable replicators. TODO optimize.
            Set<Replicator> stableReps = new HashSet<>();
            Iterator<Entry<NodeId, Replicator>> iter = replicators.entrySet().iterator();

            while (iter.hasNext()) {
                Entry<NodeId, Replicator> entry = iter.next();

                if (getMembers().contains(entry.getKey()))
                    stableReps.add(entry.getValue());
            }

            Replicator[] arr = stableReps.toArray(Replicator[]::new);

            Arrays.sort(arr, new Comparator<Replicator>() {
                @Override
                public int compare(Replicator o1, Replicator o2) {
                    return Long.compare(o2.getRepCntr(), o1.getRepCntr());
                }
            });

            // Ignore first element.
            safeCntr = arr[maj - 2].getRepCntr();
            safeTs = arr[maj - 2].getRepTs();
        }
    }

    public int majority() {
        return members.size() / 2 + 1;
    }

    public void setRepTs(Timestamp now) {
        this.repTs = now;
    }

    public Timestamp getSafeTs() {
        return this.safeTs;
    }

    public long getSafeCntr() {
        return this.safeCntr;
    }

    public @Nullable Set<NodeId> getState() {
        return members;
    }

    public void setRepCntr(long cntr) {
        this.repCntr = cntr;
        assert repCntr >= safeCntr;
    }

    public void setSafeTs(Timestamp now) {
        this.safeTs = now;
        assert repCntr >= safeCntr;
    }

    public void reset() {
        setState(Collections.emptySet());
        replicators.clear();
        repCntr = safeCntr = 0;
        repTs = safeTs = Timestamp.min();
    }

    public void setSnapshot(NavigableMap<Timestamp, Replicate> snapshot) {
        for (Entry<Timestamp, Replicate> entry : snapshot.entrySet()) {
            accept(entry.getKey(), entry.getValue(), false);
        }
    }

    public void addMember(NodeId sender) {
        boolean added = members.add(sender);

        assert added : "Must not be stable node";
    }

    public NavigableMap<Timestamp, Replicate> snapshot(Timestamp low, Timestamp high) {
        return new TreeMap(snapIdx.subMap(low, false, high, true));
    }
}
