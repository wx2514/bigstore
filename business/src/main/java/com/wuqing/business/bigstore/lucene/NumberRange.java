package com.wuqing.business.bigstore.lucene;

/**
 * Created by wuqing on 17/6/27.
 */
@Deprecated
public class NumberRange {
    private long start;
    private long end;

    public NumberRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

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
}
