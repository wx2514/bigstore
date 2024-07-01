package com.wuqing.business.bigstore.lucene;

/**
 * Created by wuqing on 17/6/22.
 */
public class LuceneBean {

    private long number;

    private String context;

    public LuceneBean(long number, String context) {
        this.number = number;
        this.context = context;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }
}
