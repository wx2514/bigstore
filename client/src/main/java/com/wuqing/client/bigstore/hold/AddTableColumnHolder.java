package com.wuqing.client.bigstore.hold;

import com.wuqing.client.bigstore.bean.ColumnDef;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wuqing on 17/4/5.
 */
public class AddTableColumnHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    public AddTableColumnHolder(String dataBase, String table, List<ColumnDef> colList) {
        type = ADD_TABLE_COLUMN;
        this.dataBase = dataBase;
        this.table = table;
        this.colList = colList;
    }

    private String dataBase;
    private String table;
    private List<ColumnDef> colList;

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

    public List<ColumnDef> getColList() {
        return colList;
    }

    public void setColList(List<ColumnDef> colList) {
        this.colList = colList;
    }
}
