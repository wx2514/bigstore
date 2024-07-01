package com.wuqing.business.bigstore.manager;

import com.wuqing.business.bigstore.bean.TableInfo;
import com.wuqing.business.bigstore.util.CacheUtil;
import com.wuqing.client.bigstore.bean.ColumnDef;

public class CommonManager {

    /**
     * 是否是顺序字段（表分区字段）
     * @param dataBase
     * @param table
     * @param column
     * @return
     */
    public static boolean isOrderColumn(String dataBase, String table, String column) throws Exception {
        if (column == null) {
            return false;
        }
        TableInfo tableInfo = CacheUtil.readTableInfo(dataBase, table);
        ColumnDef[] cols = tableInfo.getColumnDefs();
        if (cols == null || cols.length == 0) {
            return false;
        }
        for (ColumnDef def : cols) {
            if ((def.isOrderSpace()) && column.equals(def.getName())) {
                return true;
            }
        }
        return false;
    }
}
