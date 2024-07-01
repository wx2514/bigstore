package com.wuqing.business.bigstore.lucene;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/6/30.
 */
public class LuceneResult {

    private long total;

    private List<Long> numberList = new ArrayList<Long>();

    public LuceneResult(long total, List<Long> numberList) {
        this.total = total;
        this.numberList = numberList;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<Long> getNumberList() {
        return numberList;
    }

    public void setNumberList(List<Long> numberList) {
        this.numberList = numberList;
    }
}
