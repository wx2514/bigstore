package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/3/9.
 */
public class StringDataLine implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 实际数据
     */
    private String data;

    /**
     * 行号
     */
     private int number;

    public StringDataLine(String data, int number) {
        this.data = data;
        this.number = number;
    }

    public String getData() {
        return data;
    }

    public int getNumber() {
        return number;
    }


}
