package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/17.
 */
public class QueryRange implements Serializable {
    private static final long serialVersionUID = 1L;

    private long start = Long.MIN_VALUE;
    private long end = Long.MIN_VALUE;

    public QueryRange() {

    }

    public QueryRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public QueryRange setStart(long start) {
        this.start = start;
        return this;
    }

    public long getEnd() {
        return end;
    }

    public QueryRange setEnd(long end) {
        this.end = end;
        return this;
    }

    @Override
    public String toString() {
        return "QueryRange{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
