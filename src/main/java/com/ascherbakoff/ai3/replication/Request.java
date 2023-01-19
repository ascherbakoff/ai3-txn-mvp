package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Request {
    int type;
    private Timestamp lwm;
    private Object payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }
}
