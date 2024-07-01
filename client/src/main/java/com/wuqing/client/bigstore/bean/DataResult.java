package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wuqing on 17/3/13.
 */
public class DataResult implements Serializable {

    private static final long serialVersionUID = 1L;

    public DataResult() {

    }

    public DataResult(long total, List<List<String>> datas) {
        this.total = total;
        this.datas = datas;
    }

    private int index = -1;

    /**
     * 总数
     */
    private long total;

    /**
     * 结果数据
     */
    private List<List<String>> datas = new ArrayList<List<String>>();

    private Map<String, List<FunctionData>> datasByAggregation;

    private List<String> columns = new ArrayList<String>();

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void addTotal(long total) {
        this.total += total;
    }

    public List<List<String>> getDatas() {
        return datas;
    }

    public void setDatas(List<List<String>> datas) {
        this.datas = datas;
    }

    public void addDatas(List<List<String>> datas) {
        this.datas.addAll(datas);
    }

    public Map<String, List<FunctionData>> getDatasByAggregation() {
        return datasByAggregation;
    }

    public void setDatasByAggregation(Map<String, List<FunctionData>> datasByAggregation) {
        this.datasByAggregation = datasByAggregation;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        if (columns == null) {
            return;
        }
        this.columns = new ArrayList<String>();
        this.columns.add(Constants.COLUMN_ID);
        for (String col : columns) {
            this.columns.add(col);
        }
    }

    public boolean next() {
        return (++index < datas.size());
    }

    /*public Integer getInt(int colNum, int defaultValue) {
        Integer v = getInt(colNum);
        return v == null ? defaultValue : v;
    }

    public Long getLong(int colNum, long defaultValue) {
        Long v = getLong(colNum);
        return v == null ? defaultValue : v;
    }*/

    /**
     * 列位置，从 1 开始
     * @param colNum
     * @return
     */
    public int getInt(int colNum) {
        return CommonUtil.parseInt(getString(colNum));
    }

    public long getLong(int colNum) {
        return CommonUtil.parseLong(getString(colNum));
    }

    public float getFloat(int colNum) {
        return CommonUtil.parseFloat(getString(colNum));
    }

    public String getString(int colNum) {
        if (index >= datas.size()) {
            return null;
        }
        List<String> line = datas.get(index);
        if (colNum >= line.size()) {
            return null;
        }
        String s = line.get(colNum);
        if (s != null) {
            s = s.replace(Constants.LINE_BREAK_REPLACE, '\n');
        }
        return s;
    }

    public List<String> getRow() {
        List<String> list = datas.get(index);
        for (int i = 0, k = list.size(); i < k; i++) {
            String s = list.get(i);
            if (s != null) {
                s = s.replace(Constants.LINE_BREAK_REPLACE, '\n');
                list.set(i, s);
            }

        }
        return list;
    }

}
