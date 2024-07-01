package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/9.
 */
public class LongDataLine implements Serializable, Comparable<LongDataLine> {

    private static final long serialVersionUID = 1L;

    /**
     * 实际数据
     */
    private long data;

    /**
     * 行号
     */
     private int number;

    public LongDataLine(long data, int number) {
        this.data = data;
        this.number = number;
    }

    public long getData() {
        return data;
    }

    public int getNumber() {
        return number;
    }

    @Override
    public int compareTo(LongDataLine o) {
        if (this.data - o.getData() < 0) {
            return -1;
        } else if (this.data - o.getData() > 0) {
            return 1;
        } else {
            return this.number - o.getNumber();
        }
    }
}
