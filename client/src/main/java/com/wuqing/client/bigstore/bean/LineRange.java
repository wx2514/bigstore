package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/16.
 * 行范围
 */
public class LineRange implements Serializable {

    private static final long serialVersionUID = 1L;

    private long start;

    private long end;

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public LineRange(long num) {
        this.start = num;
        this.end = num;
    }

    public LineRange(long start, long end) {
        this.start = start;
        this.end = end;
    }
}
