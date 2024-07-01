package com.wuqing.client.bigstore.bean;

import java.util.List;

/**
 * 保存时的bean对象
 */
public class StoreBean {

    public static String KEY_SPLIT = "=";

    public StoreBean(String dataBase, String table, String[] line) {
        this.dataBase = dataBase;
        this.table = table;
        this.line = line;
    }

    private String dataBase;

    private String table;

    private String[] line;

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String[] getLine() {
        return line;
    }

    public void setLine(String[] line) {
        this.line = line;
    }

    public String getKey() {
        return this.dataBase + KEY_SPLIT + this.table;
    }
}
