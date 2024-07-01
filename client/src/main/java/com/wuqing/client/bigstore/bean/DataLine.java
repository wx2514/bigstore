package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/9.
 */
public class DataLine implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 实际数据
     */
    private String data;

    /**
     * 行号
     */
     private long number;

    public DataLine(String data, long number) {
        this.data = data;
        this.number = number;
    }

    public String getData() {
        return data;
    }

    public long getNumber() {
        return number;
    }
}
