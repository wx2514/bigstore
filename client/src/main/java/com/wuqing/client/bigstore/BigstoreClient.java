package com.wuqing.client.bigstore;

import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.bean.pkg.FutureResult;
import com.wuqing.client.bigstore.bean.pkg.ResultListener;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.exception.SqlException;
import com.wuqing.client.bigstore.hold.*;
import com.wuqing.client.bigstore.hold.*;
import com.wuqing.client.bigstore.tcp.client.TcpClient;
import com.wuqing.client.bigstore.util.CommonUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BigstoreClient {

    private static Logger logger = LoggerFactory.getLogger(BigstoreClient.class);

    public static final long DEFAULT_TIME_OUT = 60000;

    private static final int HEAT_BEAT_SLEEP_SECOND = 60000;

    /**
     * 当客户端端启动时候开始计算心跳失败次数
     * 如果有成功的则 设置成 -1 后续不再计数
     * 如果连续5次都失败，设置成 -2 后续不在进行心跳检测
     */
    private static int heatCountFail = 0;

    private String host;

    private int port;

    private TcpClient client;

    private String dataBase;

    private long lastRestartTime;

    private long timeOut = DEFAULT_TIME_OUT;   //默认60秒超时

    public BigstoreClient(String host, int port) {
        this(host, port, Constants.DEFAULT_DATA_BASE);
    }

    public BigstoreClient(String host, int port, String dataBase) {
        this(host, port, dataBase, DEFAULT_TIME_OUT);
    }

    public BigstoreClient(String host, int port, String dataBase, long timeOut) {
        this.host = host;
        this.port = port;
        this.client = new TcpClient(host, port);
        this.dataBase = dataBase;
        this.timeOut = timeOut;
        //执行心跳检测
        startHeartBeat();
    }

    /**
     * 心跳检测
     * 为了兼容老版本服务端没有心跳检测的情况，所以开局连续3次都失败。就跳过不再检测
     */
    private void startHeartBeat() {
        Thread hearThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (heatCountFail == -2) {
                            logger.warn("quit heart beat thread");
                            break;
                        }
                        ResultListener<ResponseData> listener = null;
                        try {
                            MyPackage p = new MyPackage();
                            p.setData(Constants.Commond.HEART_BEAT_CMD);
                            listener = createListener(p);
                            writeAndFlush(p);
                        } catch (Exception e) {
                        }
                        //getResult里如果拿不到返回，会直接重置链接
                        ResponseData res = getResult(listener);
                        if (res == null) {
                            logger.warn("Heart Beat is Fail");
                            if (heatCountFail >= 0) {
                                heatCountFail++;
                                if (heatCountFail >= 3) {
                                    //标记成不需要心跳检测
                                    heatCountFail = -2;
                                }
                            }
                        } else {
                            //有成功的说明服务端版本已经支持心跳检测，不再进行失败检测
                            heatCountFail = -1;
                        }
                    } catch (Exception e) {
                        logger.error("run HeartBeat exceotion", e);
                    } finally {
                        try {
                            //一分钟一次心跳检测
                            Thread.sleep(HEAT_BEAT_SLEEP_SECOND);
                        } catch (InterruptedException e) {
                            logger.error("sleep fail", e);
                        }
                    }
                }
            }
        });
        hearThread.setDaemon(true);
        hearThread.setName("Heart-Beat-Thread-" + this.host);
        hearThread.start();

    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public void writeAndFlush(MyPackage pkg) {
        if (!client.isActive()) {
            restart();
        }
        ChannelFuture future = client.writeAndFlush(pkg);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.info("client-complete-fail, ", future.cause());
                }
            }
        });
    }

    /*public Object getResult(Long time) {
        return client.getResult(time);
    }

    public Object getResult(Long time, long timeout) {
        return client.getResult(time, timeout);
    }*/

    public void stopClient() {
        client.stopClient();
    }

    public ResponseData getResult(ResultListener<ResponseData> listener) {
        ResponseData res = null;
        try {
            res = listener.getResult();
        } catch (Exception e) {
            logger.error("get listener result fail", e);
        }
        if (res == null) {
            restart();
        }
        return res;
    }

    public ResponseData createTable(String table, List<ColumnDef> colList) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        MyPackage p = new MyPackage();
        p.setData(new CreateTableHolder(this.dataBase, table, colList));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData addTableColumn(String table, List<ColumnDef> colList) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        MyPackage p = new MyPackage();
        p.setData(new AddTableColumnHolder(this.dataBase, table, colList));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    private ResultListener<ResponseData> createListener(MyPackage p) {
        ResultListener<ResponseData> listener = new FutureResult<ResponseData>(this.timeOut);
        this.addResultListener(p.getTime(), listener);
        return listener;
    }

    public ResponseData loadData(String table, String filePath) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        MyPackage p = new MyPackage();
        p.setData(new LoadDataHolder(this.dataBase, table, filePath));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData asyncStoreData(String table, String[] line) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        this.checkData(line);
        MyPackage p = new MyPackage();
        p.setData(new AsyncStoreDataHolder(this.dataBase, table, line));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData asyncBatchStoreData(String table, List<String[]> lines) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        AsyncBatchStoreDataHolder asyncBatchStoreDataHolder = new AsyncBatchStoreDataHolder();
        this.checkData(lines);
        for (String[] line : lines) {
            asyncBatchStoreDataHolder.add(new AsyncStoreDataHolder(this.dataBase, table, line));
        }
        MyPackage p = new MyPackage();
        p.setData(asyncBatchStoreDataHolder);
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData asyncBatchStoreData(Map<String, List<String[]>> lines) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        AsyncBatchStoreDataHolder asyncBatchStoreDataHolder = new AsyncBatchStoreDataHolder();
        for (Map.Entry<String, List<String[]>> entry : lines.entrySet()) {
            this.checkData(entry.getValue());
            String table = entry.getKey();
            for (String[] line : entry.getValue()) {
                asyncBatchStoreDataHolder.add(new AsyncStoreDataHolder(this.dataBase, table, line));
            }
        }
        MyPackage p = new MyPackage();
        p.setData(asyncBatchStoreDataHolder);
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData storeData(String table, List<String[]> lines) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        this.checkData(lines);
        MyPackage p = new MyPackage();
        p.setData(new StoreDataHolder(this.dataBase, table, lines));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    private void checkData(List<String[]> lines) {
        if (CommonUtil.isEmpty(lines)) {
            throw new SqlException("保存的数据不能为空");
        }
        for (String[] l : lines) {
            if (CommonUtil.isEmpty(l)) {
                continue;
            }
            for (int i = 0, k = l.length; i < k; i++) {
                String s = l[i];
                if (CommonUtil.isEmpty(s)) {
                    continue;
                }
                if (s.indexOf("\n") > -1) {
                    s = s.replace('\n', Constants.LINE_BREAK_REPLACE);
                    l[i] = s;
                    /*logger.error("have line split. s:" + s);
                    throw new SqlException("保存的数据中不能有'换行符'");*/
                }
            }

        }
    }

    private void checkData(String[] line) {
        if (CommonUtil.isEmpty(line)) {
            throw new SqlException("保存的数据不能为空");
        }
        for (int i = 0, k = line.length; i < k; i++) {
            String s = line[i];
            if (CommonUtil.isEmpty(s)) {
                continue;
            }
            if (s.indexOf("\n") > -1) {
                s = s.replace('\n', Constants.LINE_BREAK_REPLACE);
                line[i] = s;
                //throw new SqlException("保存的数据中不能有'换行符'");
            }
        }
    }

    public ResponseData showDatabases() {
        MyPackage p = new MyPackage();
        p.setData(new ShowDatabasesHolder());
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData showTables() {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        MyPackage p = new MyPackage();
        p.setData(new ShowTablesHolder(this.dataBase));
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData descTable(String table) {
        if (this.dataBase == null) {
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        if (table == null) {
            throw new SqlException("数据表[table]不能为空，请指定所要查询的表");
        }
        MyPackage p = new MyPackage();
        p.setData(new DescTableHolder(this.dataBase, table));
        this.writeAndFlush(p);
        ResultListener<ResponseData> listener = createListener(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public ResponseData flushTableCache() {
        MyPackage p = new MyPackage();
        p.setData(new FlushTableCacheHolder());
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    /**
     * 默认查询整个集群（如果有多台）
     * @param condition
     * @return
     */
    public ResponseData query(Condition condition) {
        boolean queryGroup = true;
        return query(condition, queryGroup);
    }

    /**
     * 查询数据
     * @param condition
     * @param queryGroup    true:查询集群， false:查询单台
     * @return
     */
    public ResponseData query(Condition condition, boolean queryGroup) {
        if (this.dataBase != null) {
            condition.setDataBase(this.dataBase);    //强行设置dataBase;
        }
        if (condition.getDataBase() == null) {  //验证是否有dataBase；
            throw new SqlException("数据库[database]不能为空，请指定所要查询的库");
        }
        /*if (condition.getConditionSubList().isEmpty()) {
            throw new SqlException("查询条件不能为空，请追加分区字段查询");
        }*/
        MyPackage p = new MyPackage();
        QueryTableHolder holder = new QueryTableHolder(condition);
        holder.setQueryGroup(queryGroup);
        p.setData(holder);
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    public void restart() {
        if (System.currentTimeMillis() - lastRestartTime < 10000L) {    //10秒保护时间
            return;
        }
        Map<Long, ResultListener> listenersOld = this.client.getResultListeners();
        this.client.stopClient();
        this.client = new TcpClient(host, port);
        if (!CommonUtil.isEmpty(listenersOld)) {    //如果不为空，listeners也同步过去
            for (Map.Entry<Long, ResultListener> entry : listenersOld.entrySet()) {
                this.client.addResultListener(entry.getKey(), entry.getValue());
            }
        }
        lastRestartTime = System.currentTimeMillis();
    }

    public ResponseData syncData(String filePath, byte[] bytes) {
        FileData filePackage = new FileData(filePath, bytes);
        MyPackage p = new MyPackage();
        p.setData(filePackage);
        ResultListener<ResponseData> listener = createListener(p);
        this.writeAndFlush(p);
        ResponseData res = getResult(listener);
        return res;
    }

    private void addResultListener(long seq, ResultListener listener) {
        this.client.addResultListener(seq, listener);
    }


}