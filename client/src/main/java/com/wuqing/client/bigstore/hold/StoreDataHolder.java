package com.wuqing.client.bigstore.hold;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wuqing on 17/4/5.
 */
public class StoreDataHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    public StoreDataHolder(String dataBase, String table, List<String[]> lines) {
        type = STORE_DATA;
        this.dataBase = dataBase;
        this.table = table;
        this.lines = lines;
    }

    private String dataBase;
    private String table;
    private List<String[]> lines;

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

    public List<String[]> getLines() {
        return lines;
    }

    public void setLines(List<String[]> lines) {
        this.lines = lines;
    }
}
