package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class CollectResponse extends Response {
    private final Timestamp lwm;

    public CollectResponse(Timestamp lwm, Timestamp ts) {
        super(ts);
        this.lwm = lwm;
    }

    public CollectResponse(Timestamp ts, int ret) {
        super(ts, ret);
        this.lwm = null;
    }

    public Timestamp getLwm() {
        return lwm;
    }
}
