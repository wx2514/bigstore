package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

public class FieldAs implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;

    private String as;

    public FieldAs(String name, String as) {
        this.name = name;
        this.as = as;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAs() {
        return as;
    }

    public void setAs(String as) {
        this.as = as;
    }
}
