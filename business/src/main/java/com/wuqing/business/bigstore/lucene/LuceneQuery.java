package com.wuqing.business.bigstore.lucene;

/**
 * Created by wuqing on 17/6/27.
 */
public class LuceneQuery {

    private String key;

    private String value;

    public LuceneQuery(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
