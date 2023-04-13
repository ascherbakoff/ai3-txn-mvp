package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.table.VersionChainRowStore;
import java.util.Map.Entry;

public class SnapshotResponse extends Response {
    private final VersionChainRowStore<Entry<Integer, Integer>> snapshot;

    public SnapshotResponse(Timestamp ts, VersionChainRowStore<Entry<Integer, Integer>> snapshot) {
        super(ts);
        this.snapshot = snapshot;
    }

    public VersionChainRowStore<Entry<Integer, Integer>> getSnapshot() {
        return snapshot;
    }
}
