package com.wuqing.client.bigstore.hold;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/5.
 */
public class DescTableHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    private String database;

    private String table;

    public DescTableHolder(String database, String table) {
        type = DESC_TABLES;
        this.database = database;
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }
}
