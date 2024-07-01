package com.wuqing.client.bigstore.hold;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/5.
 */
public class LoadDataHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    public LoadDataHolder(String dataBase, String table, String filePath) {
        type = LOAD_DATA;
        this.dataBase =  dataBase;
        this.table = table;
        this.filePath = filePath;
    }

    private String dataBase;
    private String table;
    private String filePath;

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

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
