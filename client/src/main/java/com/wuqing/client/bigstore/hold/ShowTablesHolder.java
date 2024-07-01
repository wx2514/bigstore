package com.wuqing.client.bigstore.hold;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/5.
 */
public class ShowTablesHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    private String database;

    public ShowTablesHolder(String database) {
        type = Holder.SHOW_TABLES;
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }
}
