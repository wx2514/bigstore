package com.wuqing.client.bigstore.hold;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by wuqing on 17/4/5.
 */
public class AsyncStoreDataHolder extends Holder implements Serializable {

    public static String KEY_SPLIT = "=";

    private static final long serialVersionUID = 1L;

    public AsyncStoreDataHolder(String dataBase, String table, String[] line) {
        type = ASYNC_STORE_DATA;
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

    @Override
    public String toString() {
        return "AsyncStoreDataHolder{" +
                "dataBase='" + dataBase + '\'' +
                ", table='" + table + '\'' +
                ", line=" + Arrays.toString(line) +
                '}';
    }
}
