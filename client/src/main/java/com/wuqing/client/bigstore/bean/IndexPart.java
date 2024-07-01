package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/3.
 * 段索引，存放此段是否存在数据
 */
public class IndexPart implements Serializable {
    private static final long serialVersionUID = 1L;

    private long start;
    private long end;
    private boolean exsit;

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

    public boolean isExsit() {
        return exsit;
    }

    public void setExsit(boolean exsit) {
        this.exsit = exsit;
    }
}
