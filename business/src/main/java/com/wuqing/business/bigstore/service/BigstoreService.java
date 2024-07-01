package com.wuqing.business.bigstore.service;

import com.wuqing.business.bigstore.bean.TableInfo;
import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.exception.BusinessException;
import com.wuqing.business.bigstore.manager.QueryManager;
import com.wuqing.business.bigstore.manager.StoreManager;
import com.wuqing.business.bigstore.util.CacheUtil;
import com.wuqing.business.bigstore.util.DataBaseUtil;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wuqing on 17/2/17.
 * 数据库操作类
 */
public class BigstoreService {

    private final static Logger logger = LoggerFactory.getLogger(BigstoreService.class);

    public static void flushTableCache() {
        TableCache.clearTableCahce();
    }

    public static void loadTable(final String dataBase, final String table, String filePath) throws Exception {
        StoreManager.loadTable(dataBase, table, filePath);
    }

    public static DataResult query(final Condition condOrg) throws Exception {
        return QueryManager.query(condOrg);
    }

    public static List<String[]> storeTable(final String dataBase, final String table, List<String[]> lines) throws Exception {
        return StoreManager.storeTable(dataBase, table, lines);

    }

    public static DataResult descTable(String database, String table) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(database, table);
        if (tableInfo == null) {
            throw new BusinessException("unknow table: " + table);
        }
        List<List<String>> datas = new ArrayList<List<String>>();
        for (ColumnDef def : tableInfo.getColumnDefs()) {
            datas.add(CommonUtil.asList(def.getName(), def.getDesc()));
        }
        return new DataResult(datas.size(), datas);
    }

    public static DataResult showDatabases() {
        File dbFile = new File(Params.getBaseDir());
        List<String> databases = new ArrayList<String>();
        for (File f : dbFile.listFiles()) {
            if (f.isFile()) {
                continue;
            }
            databases.add(f.getName());
        }
        List<List<String>> data = new ArrayList<List<String>>();
        data.add(databases);
        return new DataResult(data.size(), data);
    }

    public static DataResult showTabless(String dataBase) {
        File dbFile = new File(Params.getBaseDir() + "/" + dataBase);
        List<String> databases = new ArrayList<String>();
        for (File f : dbFile.listFiles()) {
            if (f.isFile()) {
                continue;
            }
            databases.add(f.getName());
        }
        List<List<String>> data = new ArrayList<List<String>>();
        data.add(databases);
        return new DataResult(data.size(), data);
    }

    /**
     * 创建表结构
     * @param table
     * @param colList
     */
    public static void createTable(String dataBase, String table, List<ColumnDef> colList) throws BusinessException {
        String tablePath = DataBaseUtil.getTablePath(dataBase, table);
        String tableDirDesc = tablePath + "/desc.txt";
        if ((new File(tablePath)).exists()) {
            logger.error("table is exsit, table:" + table);
            throw new BusinessException("table is exsit, table:" + table);
        }
        List<String> list = new ArrayList<String>();
        for (ColumnDef col : colList) {
            list.add(col.toString());
        }
        FileUtil.writeFile(tableDirDesc, list, false);
        String tableSeq = tablePath + "/" + Constants.TABLE_SEQUENCE;
        FileUtil.writeFile(tableSeq, CommonUtil.asList("0"), false);
        String rows = tablePath + "/rows/";
        new File(rows).mkdirs();
    }

    public static void addTableColumn(String dataBase, String table, List<ColumnDef> colList) throws Exception {
        String tablePath = DataBaseUtil.getTablePath(dataBase, table);
        String tableDirDesc = tablePath + "/desc.txt";
        if (!(new File(tablePath)).exists()) {
            logger.error("table is not exsit, table:" + table);
            throw new BusinessException("table is not exsit, table:" + table);
        }
        List<String> list = new ArrayList<String>();
        TableInfo tbInfo = CacheUtil.readTableInfo(dataBase, table);
        Set<String> colNames = new HashSet<>();
        for (ColumnDef col : tbInfo.getColumnDefs()) {
            colNames.add(col.getName());
        }
        for (ColumnDef col : colList) {
            if (!colNames.add(col.getName())) {
                throw new BusinessException("column is exsit, column:" + col.getName());
            }
            list.add(col.toString());
        }
        FileUtil.writeFile(tableDirDesc, list, true);
        //清理缓存
        TableCache.removeTableCache(dataBase, table);
    }

    public static void main(String[] args) throws Exception {

    }


}
