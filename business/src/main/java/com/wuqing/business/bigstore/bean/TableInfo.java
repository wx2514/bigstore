package com.wuqing.business.bigstore.bean;

import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.config.Constants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/14.
 */
public class TableInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 列元素集合
     */
    private ColumnDef[]columnDefs = null;

    /**
     * 总个数
     * 下一个自增ID
     */
    private long nextId;

    /**
     * 索引列
     */
    private List<String> indexCols = new ArrayList<String>();

    public TableInfo(ColumnDef[] columnDefs, long nextId) {
        this.columnDefs = columnDefs;
        this.nextId = nextId;
        if (columnDefs != null) {
            for (ColumnDef col : columnDefs) {
                if (col.isLong() || col.isEnum()) { //整型，枚举类型辨识度比较高，目前将这俩种字段的索引优先进入缓存
                    indexCols.add(col.getName());
                }
            }
        }
    }

    public ColumnDef[] getColumnDefs() {
        return columnDefs;
    }

    public ColumnDef getColumnDef(String column) {
        if (Constants.COLUMN_ID.equals(column)) {
            int length = String.valueOf(ServerConstants.PARK_SIZ).length() - 1;
            return new ColumnDef(Constants.COLUMN_ID, ColumnDef.LONG_TYPE).setLength(length);
        }
        if (columnDefs == null || column == null) {
            return null;
        }
        for (ColumnDef def : columnDefs) {
            if (column.equals(def.getName())) {
                return def;
            }
        }
        return null;
    }

    /**
     * 获取第一个分区字段（数值型）
     * @return
     */
    public ColumnDef getOrderColumn() {
        for (ColumnDef def : columnDefs) {
            if (def.isOrder()) {
                return def.clone();
            }
        }
        return null;
    }

    /*public void setColumnDefs(ColumnDef[] columnDefs) {
        this.columnDefs = columnDefs;
    }*/

    public long getNextId() {
        return nextId;
    }

    public void setNextId(long nextId) {
        this.nextId = nextId;
    }

    public boolean isIndexCol(String name) {
        return indexCols.contains(name);
    }
}
