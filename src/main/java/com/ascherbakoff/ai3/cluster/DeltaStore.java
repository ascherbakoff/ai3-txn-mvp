package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Replicate;
import java.util.NavigableMap;

/**
 * An abstraction for storing replicated data with delta snap support.
 */
public interface DeltaStore {
    void put(Timestamp repTs, Replicate replicate);

    NavigableMap<Timestamp, Replicate> snapshot(Timestamp low, Timestamp high);

    void compact(Timestamp compactTs);

    Timestamp compactTs();
}
