package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/17.
 */
public class QueryRangeByte implements Serializable {
    private static final long serialVersionUID = 1L;

    private byte[] start;
    private byte[] end;

    public QueryRangeByte(byte[] start, byte[] end) {
        this.start = start;
        this.end = end;
    }

    public byte[] getStart() {
        return start;
    }

    public byte[] getEnd() {
        return end;
    }
}
