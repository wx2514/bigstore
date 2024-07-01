package com.wuqing.ui.bigstore.query;

import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.ui.bigstore.bean.UiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(QueryUtil.class);

    private static final BigstoreClient CLIENT = new BigstoreClient("127.0.0.1", Constants.PORT, Constants.DEFAULT_DATA_BASE);

    public static List<String> showDatabase() {
        List<String> result = new ArrayList<>();
        ResponseData responseData = CLIENT.showDatabases();
        DataResult data = responseData.getData();
        while (data.next()) {
            for (String s : data.getRow()) {
                result.add(s);
            }
        }
        return result;
    }

    public static UiResult<List<List<String>>> showTables() {
        UiResult<List<List<String>>> uiResult = new UiResult<>();
        ResponseData response = CLIENT.showTables();
        if (response.isSuccess()) {
            DataResult res = response.getData();
            List<List<String>> list = res.getDatas();
            List<List<String>> resList = new ArrayList<>();
            resList.add(Arrays.asList("表名"));
            if (!CommonUtil.isEmpty(list)) {
                for (String s : list.get(0)) {
                    resList.add(Arrays.asList(s));
                }
            }
            uiResult.setSuccessData(resList);
        } else {
            uiResult.setSuccess(false);
            uiResult.setError("查询失败, 服务端异常信息:" + response.getErrorMsg());
        }
        return uiResult;
    }

    public static void useDatabase(String database) {
        CLIENT.setDataBase(database);
    }

    public static UiResult asyncStoreData(String table, String[] line) {
        UiResult uiResult = new UiResult<>();
        try {
            CLIENT.asyncStoreData(table, line);
            uiResult.setSuccess(true);
        } catch (Exception e) {
            LOGGER.error("async store data fail", e);
            uiResult.setError("async store data fail");
        }
        return uiResult;
    }

    public static UiResult<List<List<String>>> query(String sql) {
        UiResult<List<List<String>>> uiResult = new UiResult<>();
        if (CommonUtil.isEmpty(sql)) {
            uiResult.setError("SQL 不能为空");
            return uiResult;
        }
        try {
            sql = sql.trim();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            //简单点处理吧，不管了
            if (sql.toUpperCase().startsWith("SHOW")) {
                sql = sql.replace("  ", " ");
                sql = sql.replace("  ", " ");
                sql = sql.replace("  ", " ");
            }
            if ("SHOW TABLES".equals(sql.toUpperCase())) {
                return showTables();
            }
            Condition condition = new Condition();
            condition.setSql(sql);
            ResponseData response = CLIENT.query(condition);
            if (response.isSuccess()) {
                DataResult res = response.getData();
                List<List<String>> list = res.getDatas();
                list.add(0, res.getColumns());
                uiResult.setSuccessData(list);
                uiResult.setTotal(res.getTotal());
            } else {
                uiResult.setSuccess(false);
                uiResult.setError("查询失败, 服务端异常信息:" + response.getErrorMsg());
            }
        } catch (Exception e) {
            LOGGER.error("query fail.", e);
            uiResult.setError("查询失败, 异常信息:" + e.getMessage());
        }
        return uiResult;
    }

    public static void main(String[] args) {
        List<String> list = showDatabase();
        System.out.println(list);

        useDatabase("def");

        System.exit(0);
    }
}
