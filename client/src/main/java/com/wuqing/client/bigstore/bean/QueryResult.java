package com.wuqing.client.bigstore.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/10.
 */
public class QueryResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<DataLine> data = null;
    private long total = 0L;
    private DataPack dir;
    private List<LineRange> lineRangeList = null;

    public QueryResult() {

    }

    public QueryResult(long total, DataPack dir) {
        this.total = total;
        this.dir = dir;
    }

    public List<DataLine> getData() {
        return data;
    }

    public QueryResult setData(List<DataLine> data) {
        this.data = data;
        return this;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        if (total > 0) {
            this.total = total;
        } else {
            this.total = 0L;
        }
    }

    public DataPack getDir() {
        return dir;
    }

    public void setDir(DataPack dir) {
        this.dir = dir;
    }

    public List<LineRange> getLineRangeList() {
        return lineRangeList;
    }

    public QueryResult addLineRangeList(LineRange lineRange) {
        if (this.lineRangeList == null) {
            this.lineRangeList = new ArrayList<LineRange>();
        }
        this.lineRangeList.add(lineRange);
        return this;
    }

    public QueryResult setLineRangeList(List<LineRange> lineRangeList) {
        this.lineRangeList = lineRangeList;
        return this;
    }

    public void addLineAtLast(long line) {
        int size = this.lineRangeList.size();
        if (size > 0) {
            LineRange last = this.lineRangeList.get(size - 1);
            if (last.getEnd() == line - 1) {    //如果是连续的更新end
                last.setEnd(line);
            } else {
                this.lineRangeList.add(new LineRange(line));
            }
        } else {
            this.lineRangeList.add(new LineRange(line));
        }
        this.total++;
    }
}
