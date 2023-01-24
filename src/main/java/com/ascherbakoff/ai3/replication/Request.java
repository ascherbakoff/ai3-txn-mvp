package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Request {
    private Timestamp ts;
    private Timestamp lwm;
    private Command payload;

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Timestamp getTs() {
        return ts;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
    }

    public void setPayload(Command payload) {
        this.payload = payload;
    }

    public Command getPayload() {
        return payload;
    }

    public enum Type {
        SYNC, DATA
    }
}
