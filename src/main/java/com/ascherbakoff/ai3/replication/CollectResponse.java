package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class CollectResponse extends Response {
    private final long repCntr;

    public CollectResponse(long repCntr, Timestamp ts) {
        super(ts);
        this.repCntr = repCntr;
    }

    public long getRepCntr() {
        return repCntr;
    }
}
