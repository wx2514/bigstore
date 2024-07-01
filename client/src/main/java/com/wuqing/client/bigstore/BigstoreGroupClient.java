package com.wuqing.client.bigstore;

import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.HostConfig;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BigstoreGroupClient {

    private static Logger logger = LoggerFactory.getLogger(BigstoreClient.class);

    /**
     * 集群客户端，集群下有几个服务器就有几个
     */
    private final BigstoreClient[] bigstoreClients;

    /**
     * 集群服务端数量
     */
    private int clientNum;

    private int current;

    /**
     * 按表打散
     */
    private int[] storeCurrent = new int[10240];

    public BigstoreGroupClient(String dataBase, long timeOut, HostConfig... clientConfigs) {
        clientNum = clientConfigs.length;
        bigstoreClients = new BigstoreClient[clientNum];
        for (int i = 0; i < clientConfigs.length; i++) {
            HostConfig hc = clientConfigs[i];
            bigstoreClients[i] = new BigstoreClient(hc.getHost(), hc.getPort(), dataBase, timeOut);
        }
    }

    public BigstoreGroupClient(String dataBase, HostConfig... clientConfigs) {
        clientNum = clientConfigs.length;
        bigstoreClients = new BigstoreClient[clientConfigs.length];
        for (int i = 0; i < clientConfigs.length; i++) {
            HostConfig hc = clientConfigs[i];
            bigstoreClients[i] = new BigstoreClient(hc.getHost(), hc.getPort(), dataBase);
        }
    }

    public ResponseData storeData(String table, List<String[]> lines) {
        return chooseStoreClient(table).storeData(table, lines);
    }

    public ResponseData asyncStoreData(String table, String[] line) {
        return chooseStoreClient(table).asyncStoreData(table, line);
    }

    public ResponseData asyncBatchStoreData(String table, List<String[]> lines) {
        return chooseStoreClient(table).asyncBatchStoreData(table, lines);
    }

    /**
     * 这个方法最不到按表打散，不建议使用
     * @param lines
     * @return
     */
    public ResponseData asyncBatchStoreData(Map<String, List<String[]>> lines) {
        return chooseStoreClient(0).asyncBatchStoreData(lines);
    }


    public ResponseData addTableColumn(String table, List<ColumnDef> colList) {
        ResponseData response = new ResponseData();
        response.setSuccess(true);
        for (BigstoreClient client : bigstoreClients) {
            ResponseData responseData = client.addTableColumn(table, colList);
            if (!responseData.isSuccess()) {
                response.setSuccess(false);
                if (!CommonUtil.isEmpty(responseData.getErrorMsg())) {
                    response.setErrorMsg(responseData.getErrorMsg());
                }
            }
        }
        return response;
    }

    public ResponseData createTable(String table, List<ColumnDef> colList) {
        ResponseData response = new ResponseData();
        response.setSuccess(true);
        for (BigstoreClient client : bigstoreClients) {
            ResponseData responseData = client.createTable(table, colList);
            if (!responseData.isSuccess()) {
                response.setSuccess(false);
                if (!CommonUtil.isEmpty(responseData.getErrorMsg())) {
                    response.setErrorMsg(responseData.getErrorMsg());
                }
            }
        }
        return response;
    }

    public ResponseData flushTableCache() {
        ResponseData response = new ResponseData();
        response.setSuccess(true);
        for (BigstoreClient client : bigstoreClients) {
            ResponseData responseData = client.flushTableCache();
            if (!responseData.isSuccess()) {
                response.setSuccess(false);
                if (!CommonUtil.isEmpty(responseData.getErrorMsg())) {
                    response.setErrorMsg(responseData.getErrorMsg());
                }
            }
        }
        return response;
    }

    public ResponseData stopClient() {
        ResponseData response = new ResponseData();
        response.setSuccess(true);
        for (BigstoreClient client : bigstoreClients) {
            client.stopClient();
        }
        return response;
    }

    public void setDataBase(String dataBase) {
        for (BigstoreClient client : bigstoreClients) {
            client.setDataBase(dataBase);
        }
    }

    public ResponseData descTable(String table) {
        return chooseClient().descTable(table);
    }

    public ResponseData query(Condition condition) {
        return chooseClient().query(condition);
    }

    public ResponseData showDatabases() {
        return chooseClient().showDatabases();
    }

    public ResponseData showTables() {
        return chooseClient().showTables();
    }

    private BigstoreClient chooseClient() {
        //这里为了效率 不考虑并发的原子性了，少量的负载不均匀也可以接受
        int i = ++current % clientNum;
        current = i;
        return bigstoreClients[i];
    }

    private BigstoreClient chooseStoreClient(String table) {
        int idx = Math.abs(table.hashCode()) % storeCurrent.length;
        return chooseStoreClient(idx);
    }

    private BigstoreClient chooseStoreClient(int idx) {
        //这里为了效率 不考虑并发的原子性了，少量的负载不均匀也可以接受
        int i = ++storeCurrent[idx] % clientNum;
        storeCurrent[idx] = i;
        return bigstoreClients[i];
    }


}
