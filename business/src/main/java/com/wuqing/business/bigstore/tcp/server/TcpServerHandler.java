package com.wuqing.business.bigstore.tcp.server;

import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.cache.QueryCache;
import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.exception.ThrowBusinessException;
import com.wuqing.business.bigstore.manager.QueryManager;
import com.wuqing.business.bigstore.run.StartMain;
import com.wuqing.business.bigstore.service.AsyncService;
import com.wuqing.business.bigstore.service.BigstoreService;
import com.wuqing.business.bigstore.util.*;
import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.hold.*;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import com.wuqing.business.bigstore.util.FileLockUtil;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.business.bigstore.util.GZipUtil;
import com.wuqing.business.bigstore.util.PoolUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class TcpServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger logger = LoggerFactory.getLogger(TcpServerHandler.class);
    //private final static Logger queryTimeLogger = LoggerFactory.getLogger("query-time-log");

    private static final Map<String, BigstoreClient> CLIENT_MAP = new ConcurrentHashMap<String, BigstoreClient>();

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null || !(msg instanceof MyPackage)) {
            System.err.println("server msg is invalid, msg:" + msg);
            return;
        }
        final MyPackage pkg = (MyPackage) msg;
        if ("stop".equals(pkg.getData())) { //stop命令
            //logger.debug("receive commond stop");
            StartMain.server.stopServer();
            CommonUtil.sleep(2 * 1000);    //休眠2秒等待结束
            AsyncService.stop();
            System.exit(0);
            return;
        }
        if (pkg.getData() instanceof FileData) {
            dowithSyncData(ctx, pkg);
        } else if (pkg.getData() instanceof String) {
            if (Constants.Commond.HEART_BEAT_CMD.equals(pkg.getData())) {
                //心跳包，直接回写
                ResponseData res = new ResponseData();
                res.setSuccess(true);
                pkg.setData(res);
                ctx.writeAndFlush(pkg);
            }
        } else {
            dowithRequest(ctx, pkg);
        }
    }

    private void dowithRequest(final ChannelHandlerContext ctx, final MyPackage pkg) {
        PoolUtil.NETTY_FIX_POLL.execute(new Runnable() {
            @Override
            public void run() {
                //long start = System.currentTimeMillis();
                try {
                    if (pkg.getData() instanceof Holder) {
                        Holder holder = (Holder) pkg.getData();
                        if (holder.getType() == Holder.QUERY) { //查询
                            //logger.debug("receive commond query");
                            QueryTableHolder query = (QueryTableHolder) holder;
                            boolean queryGroup = query.isQueryGroup();
                            if (CommonUtil.isEmpty(Params.getGroupIps())) { //如果本身就没有集合，则设置查询群组为false
                                queryGroup = false;
                            }
                            DataResult dataResult = null;
                            if (queryGroup) {
                                boolean normal = true;  //默认普通模式
                                for (String s : query.getCondition().getFields()) {
                                    if (s.indexOf("(") > -1 && s.indexOf(")") > -1) {
                                        normal = false;
                                        break;
                                    }
                                }
                                if (normal) {
                                    //普通字段查询
                                    dataResult = queryGroup(query.getCondition());
                                } else {
                                    //聚合字段查询
                                    dataResult = queryGroupByFunction(query.getCondition());
                                }
                            } else {
                                dataResult = BigstoreService.query(query.getCondition());
                            }
                            if (query.isQueryGroup()) {
                                //处理嵌套函数，数据合并的时候其实都是 根据内部的聚合函数来合并的，所以最后返回的时候再处理一次嵌套函数，否则处理了也无效（但不会有错）
                                QueryManager.doWithOtherFunction(dataResult, query.getCondition());
                                //返回给真正的客户端的数据，把中间临时数据清空 (业务侧客户端请求时候 queryGroup=true)
                                dataResult.setDatasByAggregation(null);
                            }
                            ResponseData res = new ResponseData();
                            res.setSuccessData(dataResult);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.CREATE) { //建表
                            //logger.debug("receive commond create table");
                            CreateTableHolder create = (CreateTableHolder) holder;
                            BigstoreService.createTable(create.getDataBase(), create.getTable(), create.getColList());
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.LOAD_DATA) {  //本地load
                            //logger.debug("receive commond load data");
                            LoadDataHolder load = (LoadDataHolder) holder;
                            BigstoreService.loadTable(load.getDataBase(), load.getTable(), load.getFilePath());
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.STORE_DATA) { //批量保存数据
                            //logger.debug("receive commond store data");
                            StoreDataHolder store = (StoreDataHolder) holder;
                            List<String[]> failList = BigstoreService.storeTable(store.getDataBase(), store.getTable(), store.getLines());
                            ResponseData res = new ResponseData();
                            if (CommonUtil.isEmpty(failList)) {
                                res.setSuccess(true);
                            } else {
                                List<List<String>> failData = new ArrayList<>();
                                for (String[] arr : failList) {
                                    failData.add(Arrays.asList(arr));
                                }
                                DataResult data = new DataResult(failData.size(), failData);
                                res.setSuccessData(data);
                                res.setSuccess(false);
                                res.setErrorMsg("some data write fail");
                            }
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.ASYNC_STORE_DATA) { //异步保存数据
                            AsyncStoreDataHolder store = (AsyncStoreDataHolder) holder;
                            AsyncService.addStoreBean(store);
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.ASYNC_BATCH_STORE_DATA) { //异步批量保存数据
                            AsyncBatchStoreDataHolder store = (AsyncBatchStoreDataHolder) holder;
                            AsyncService.addStoreBean(store);
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.SHOW_DATABASES) {
                            DataResult dataResult = BigstoreService.showDatabases();
                            ResponseData res = new ResponseData();
                            res.setSuccessData(dataResult);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.SHOW_TABLES) {
                            ShowTablesHolder showTablesHolder = (ShowTablesHolder) holder;
                            DataResult dataResult = BigstoreService.showTabless(showTablesHolder.getDatabase());
                            ResponseData res = new ResponseData();
                            res.setSuccessData(dataResult);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.DESC_TABLES) {
                            DescTableHolder descTableHolder = (DescTableHolder) holder;
                            DataResult dataResult = BigstoreService.descTable(descTableHolder.getDatabase(), descTableHolder.getTable());
                            ResponseData res = new ResponseData();
                            res.setSuccessData(dataResult);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.FLUSH_TABLE_CACHE) {
                            FlushTableCacheHolder flushTableHolder = (FlushTableCacheHolder) holder;
                            BigstoreService.flushTableCache();
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        } else if (holder.getType() == Holder.ADD_TABLE_COLUMN) { //建表
                            //logger.debug("receive commond create table");
                            AddTableColumnHolder create = (AddTableColumnHolder) holder;
                            BigstoreService.addTableColumn(create.getDataBase(), create.getTable(), create.getColList());
                            ResponseData res = new ResponseData();
                            res.setSuccess(true);
                            pkg.setData(res);
                            ctx.writeAndFlush(pkg);
                        }
                    }
                } catch (Exception e) {
                    logger.error("handle message fail.", e);
                    ResponseData res = new ResponseData();
                        /*if (e instanceof BusinessException) {
                            res.setErrorMsg(e.getMessage());
                        } else {
                            res.setErrorMsg("执行异常");
                        }*/
                    res.setErrorMsg(e.getMessage());
                    pkg.setData(res);
                    ctx.writeAndFlush(pkg);
                }
                //logger.debug("execute commond time:" + (System.currentTimeMillis() - start));
            }
        });
    }

    /**
     * 处理主从同步
     * @param ctx
     * @param pkg
     */
    private void dowithSyncData(final ChannelHandlerContext ctx, final MyPackage pkg) {
        PoolUtil.NETTY_SYNC_FILE_FIX_POLL.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    FileData fileData = (FileData) pkg.getData();
                    if (Params.isMergerData()) {    //合并数据，当前不建议使用
                        mergerData(fileData);
                    } else {
                        File f = new File(Params.getBaseDir() + fileData.getFilePath());
                        File dp = f.getParentFile();
                        if (!dp.exists()) {
                            dp.mkdirs();
                        }
                        ReentrantLock lock = FileLockUtil.getLock(f.getPath());
                        lock.lock();
                        try {
                            FileUtil.writeByte(f, false, fileData.getFileData());
                            String path = f.getPath();
                            if (path.startsWith(Params.getBaseDir())) {
                                String dataBase = null;
                                String table = null;
                                path = path.substring(Params.getBaseDir().length());
                                int idx = path.indexOf("/");
                                if (idx > -1) {
                                    dataBase = path.substring(0, idx);
                                    path = path.substring(idx + 1);
                                }
                                idx = path.indexOf("/");
                                if (idx > -1) {
                                    table = path.substring(0, idx);
                                    path = path.substring(idx + 1);
                                }
                                idx = path.indexOf("/");
                                if (idx == -1) {    //如果是文件了
                                    if (Constants.TABLE_SEQUENCE.equals(path)) {
                                        //重写之后，删除索引缓存
                                        TableCache.removeTableCache(dataBase, table);
                                    }
                                    if (path.endsWith(Constants.COL_ENUM_TXT)) {
                                        //重写之后，删除枚举索引缓存
                                        String col = path.substring(0, path.length() - Constants.COL_ENUM_TXT.length());
                                        TableCache.removeEnumInfo(dataBase, table, col);
                                    }
                                }
                                //主从同步的时候，只会满快的时候才会同步数据和索引，只会满space的时候才会同步分区索引，索引暂时不要清理以下缓存
                                //QueryCache.clear(dataBase, table, column, dirName); //查询查询缓存
                                //DataCache.remove(dataBase, table, dirName, column); //删除数据块DP缓存
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                    //返回成功
                    ResponseData res = new ResponseData();
                    res.setSuccess(true);
                    pkg.setData(res);
                    ctx.writeAndFlush(pkg);
                } catch (Exception e) {
                    logger.error("handle message fail.", e);
                    ResponseData res = new ResponseData();
                        /*if (e instanceof BusinessException) {
                            res.setErrorMsg(e.getMessage());
                        } else {
                            res.setErrorMsg("执行异常");
                        }*/
                    res.setErrorMsg(e.getMessage());
                    pkg.setData(res);
                    ctx.writeAndFlush(pkg);
                }
            }
        });
    }

    private void mergerData(FileData fileData) throws IOException {
        File f = new File(Params.getBaseDir() + fileData.getFilePath());
        File dp = f.getParentFile();
        if (!dp.exists()) {
            dp.mkdirs();
        }
        String space = dp.getParent();
        final String mergerPath = space + "/" + f.getName();
        ReentrantLock lock = FileLockUtil.getLock(mergerPath);
        lock.lock();
        try {
            File mergerFile = new File(mergerPath);
            int byLenth = ServerConstants.DIR_FORMAT.length() + 3;
            byte[] head = new byte[byLenth];
            int count = 0;
            //bufferedStream.write(ServerConstants.DIR_FLAG_START);
            head[count] = ServerConstants.DIR_FLAG_START;
            count++;
            //bufferedStream.write(dd.getParentFile().getName().getBytes());
            byte[] dirNameBytes = dp.getName().getBytes();
            System.arraycopy(dirNameBytes, 0, head, count, dirNameBytes.length);
            count += ServerConstants.DIR_FORMAT.length();
            //bufferedStream.write(ServerConstants.DIR_FLAG_END);
            head[count] = ServerConstants.DIR_FLAG_END;
            count++;
            //bufferedStream.write(ServerConstants.LINE_SEPARATOR);
            head[count] = ServerConstants.LINE_SEPARATOR;
            FileUtil.writeByte(mergerFile, true, head, fileData.getFileData());

            File merger = new File(space + "/" + FileConfig.MERGER_FLAG);
            if (!merger.exists()) {
                merger.createNewFile();
            }
            boolean isLast = dp.getName().endsWith("99");
            if (Params.isSyncCompress() && isLast) {
                //进行数据压缩
                PoolUtil.COMPRESS_FIX_POLL.execute(new Runnable() {
                    @Override
                    public void run() {
                        byte[] bt = GZipUtil.read2Byte(mergerPath);
                        bt = SnappyUtil.compress(bt);
                        File newFile= new File(mergerPath + ".tmp");
                        FileUtil.writeByte(newFile, bt);
                        newFile.renameTo(new File(mergerPath));
                    }
                });
            }

        } finally {
            lock.unlock();
        }
    }

    private DataResult queryGroup(final Condition condition) throws Exception {
        final String dataBase = condition.getDataBase();
        String my = Params.getLocalIp() + Constants.IP_PORT_SPLIT + Params.getPort();
        String[] ipPorts = Params.getGroupIpArray();
        boolean first = true;
        List<Future<DataResult>> future = new ArrayList<Future<DataResult>>();
        for (final String its : ipPorts) {
            final Condition con = (Condition) condition.clone();
            if (!first) {   //如果不是第一次，则只查询数量
                con.setStart(0);
                con.setLimit(0);
            }
            final boolean isThis = my.equals(its);
            Future<DataResult> f = PoolUtil.GROUP_QUERY_POLL.submit(new Callable<DataResult>() {
                @Override
                public DataResult call() throws Exception {
                    try {
                        if (isThis) {
                            return BigstoreService.query(con);
                        } else {
                            BigstoreClient client = getClient(its, dataBase);
                            ResponseData responseData = client.query(con, false);
                            return responseData.getData();
                        }
                    } catch (Exception e) {
                        logger.error("group query count fail", e);
                        return null;
                    }
                }
            });
            future.add(f);
            first = false;
        }
        first = true;
        DataResult dataFirst = null;    //第一个结果数据
        long total = 0;
        List<List<String>> datas = new ArrayList<List<String>>();
        int start = condition.getStart();
        int limit = condition.getLimit();
        List<Condition> conList = new ArrayList<Condition>();
        for (Future<DataResult> f : future) {
            DataResult dataResult = f.get();
            if (first) {
                dataFirst = dataResult;
            }
            long count = dataResult == null ? 0 : dataResult.getTotal();
            total += count;     //统计总数
            final Condition con = (Condition) condition.clone();
            if (count == 0) {
                con.setStart(0);
                con.setLimit(0);
            } else if (start > count) {
                con.setStart(0);
                con.setLimit(0);
                start -= count;
            } else {
                con.setStart(start);
                int thisLimit = (int) Math.min((count - start), limit);
                con.setLimit(thisLimit);
                start = 0;
                limit -= thisLimit;
            }
            conList.add(con);
            first = false;
        }
        first = true;
        final DataResult dataFirstExe = dataFirst;
        future.clear(); //将之前的查询记录清理掉
        for (int i = 0, k = ipPorts.length; i < k; i++) {
            final Condition con = conList.get(i);
            if (con.getLimit() == 0) {
                first = false;
                continue;
            }
            final String its = ipPorts[i];
            final boolean isThis = my.equals(its);
            final boolean judgeFirst = first;
            Future<DataResult> f = PoolUtil.GROUP_QUERY_POLL.submit(new Callable<DataResult>() {
                @Override
                public DataResult call() throws Exception {
                    try {
                        if (judgeFirst) {
                            return dataFirstExe;
                        } else if (isThis) {
                            return BigstoreService.query(con);
                        } else {
                            BigstoreClient client = getClient(its, dataBase);
                            ResponseData responseData = client.query(con, false);
                            return responseData.getData();
                        }
                    } catch (Exception e) {
                        logger.error("group query data fail", e);
                        return null;
                    }
                }
            });
            future.add(f);
            first = false;
        }
        List<String> columns = new ArrayList<String>();
        for (Future<DataResult> f : future) {
            DataResult dataResult = f.get();
            if (dataResult == null) {
                continue;
            }
            List<List<String>> dds = dataResult.getDatas();
            if (dds != null) {
                datas.addAll(dds);
            }
            List<String> col = dataResult.getColumns();
            if (!CommonUtil.isEmpty(col)) {
                columns = col;
            }
        }
        DataResult result = new DataResult(total, datas);
        //合并数据后，设置 columns
        int size = columns.size();
        if (size > 1) {
            result.setColumns(columns.subList(1, size));
        } else {
            logger.error("query result size of columns:" + size);
        }
        return result;
    }



    private DataResult queryGroupByFunction(final Condition condition) throws Exception {
        final String dataBase = condition.getDataBase();
        String my = Params.getLocalIp() + Constants.IP_PORT_SPLIT + Params.getPort();
        String[] ipPorts = Params.getGroupIpArray();
        List<Future<DataResult>> future = new ArrayList<Future<DataResult>>();
        for (final String its : ipPorts) {
            final Condition con = (Condition) condition.clone();
            final boolean isThis = my.equals(its);
            Future<DataResult> f = PoolUtil.GROUP_QUERY_POLL.submit(new Callable<DataResult>() {
                @Override
                public DataResult call() throws Exception {
                    try {
                        if (isThis) {
                            return BigstoreService.query(con);
                        } else {
                            BigstoreClient client = getClient(its, dataBase);
                            ResponseData responseData = client.query(con, false);
                            if (responseData.getErrorMsg() != null && responseData.getErrorMsg().indexOf("ThrowBusinessException") > -1) {
                                throw new ThrowBusinessException(responseData.getErrorMsg());
                            }
                            return responseData.getData();
                        }
                    } catch (ThrowBusinessException te) {
                        throw te;
                    } catch (Exception e) {
                        logger.error("group query count fail", e);
                        return null;
                    }
                }
            });
            future.add(f);
        }
        DataResult result = null;
        Map<String, List<FunctionData>> dataMapTotal = new ConcurrentSkipListMap<>();
        for (Future<DataResult> f : future) {
            DataResult dataResult = f.get();
            if (dataResult == null) {
                continue;
            }
            if (result == null) {
                result = dataResult;
            } /*else {
                result.setTotal(result.getTotal() + dataResult.getTotal());
                result.getDatas().addAll(dataResult.getDatas());
            }*/
            Map<String, List<FunctionData>> dataMap = dataResult.getDatasByAggregation();
            if (CommonUtil.isEmpty(dataMap)) {
                continue;
            }
            dataMapTotal = mergerDataMap(dataMapTotal, dataMap);
        }
        if (!CommonUtil.isEmpty(dataMapTotal)) {
            result.setDatas(QueryManager.resultMap2List(dataMapTotal));
            result.setTotal(result.getDatas().size());
        }
        return result;
    }

    private Map<String, List<FunctionData>> mergerDataMap(Map<String, List<FunctionData>> dataMapTotal, Map<String, List<FunctionData>> dataMap) {
        if (dataMapTotal.isEmpty()) {
            return dataMap;
        }
        if (dataMap.isEmpty()) {
            return dataMapTotal;
        }
        Set<String> keySet = dataMapTotal.keySet();
        for (String key : keySet) {
            List<FunctionData> fun1List = dataMapTotal.get(key);
            List<FunctionData> fun2List = dataMap.remove(key);
            if (CommonUtil.isEmpty(fun2List)) {
                continue;
            }
            int size1 = fun1List.size();
            int size2 = fun2List.size();
            if (size1 != size2) {
                logger.error("size of function is not eq");
                continue;
            }
            for (int i = 0; i < size1; i++) {
                FunctionData data = fun1List.get(i);
                FunctionData data2 = fun2List.get(i);
                int aggregation = data.getAggregation();
                QueryManager.mergerFunctionData(data, data2, aggregation);

            }
        }
        //如果还有剩余的则全部放入总Map
        if (!CommonUtil.isEmpty(dataMap)) {
            dataMapTotal.putAll(dataMap);
        }
        return dataMapTotal;
    }

    private synchronized BigstoreClient getClient(String ipPortStr, String database) {
        String key = ipPortStr + "=" + database;
        BigstoreClient client = CLIENT_MAP.get(key);
        if (client != null) {
            return client;
        }
        String[] ipPort = ipPortStr.split(Constants.IP_PORT_SPLIT);
        client = new BigstoreClient(ipPort[0], Integer.parseInt(ipPort[1]), database);
        CLIENT_MAP.put(key, client);
        return client;
    }

    /*
     *
     * 覆盖 channelActive 方法 在channel被启用的时候触发 (在建立连接的时候)
     *
     * channelActive 和 channelInActive 在后面的内容中讲述，这里先不做详细的描述
     * */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("RamoteAddress : " + ctx.channel().remoteAddress() + " active !");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("RamoteAddress : " + ctx.channel().remoteAddress() + " inactive !");
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //System.out.println("client catch exception");
        logger.error("tcp serve caught exception", cause);
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }


}