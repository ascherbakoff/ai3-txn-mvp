package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Response {
    private Timestamp ts;
    private int ret;

    public Response(Timestamp ts) {
        this(ts, 0);
    }

    public Response(Timestamp ts, int ret) {
        this.ts = ts;
        this.ret = ret;
    }

    public int getReturn() {
        return ret;
    }

    public Timestamp getTs() {
        return ts;
    }
}
