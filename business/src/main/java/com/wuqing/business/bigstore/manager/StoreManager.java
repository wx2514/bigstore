package com.wuqing.business.bigstore.manager;

import com.alibaba.fastjson.JSON;
import com.wuqing.business.bigstore.bean.*;
import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.cache.QueryCache;
import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.exception.BusinessException;
import com.wuqing.business.bigstore.process.ProcessUtil;
import com.wuqing.business.bigstore.util.*;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.util.BloomFilter;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.KryoUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class StoreManager {

    private final static Logger storeTimeLogger = LoggerFactory.getLogger("store-time-log");

    public static void loadTable(final String dataBase, final String table, String filePath) throws Exception {
        long s = System.currentTimeMillis();
        String failPath = filePath + Constants.FILE_SUFFIX; //失败的数据存放文件
        List<String> lines = FileUtil.readAll(filePath, false);
        storeTimeLogger.debug("read data from file, time:" + (System.currentTimeMillis() - s));
        String split = "!@#$%^&*()1234567890";  //避免单列随便给的初始值
        if (lines.size() > 0) {
            String ln = lines.get(0);
            if (ln.indexOf(Constants.COLUMN_SPLIT_2) > -1) {
                split = Constants.COLUMN_SPLIT_2;
            } else if (ln.indexOf(Constants.COLUMN_SPLIT_LEVEL_1) > -1) {
                split = Constants.COLUMN_SPLIT_LEVEL_1;
            }
        }
        List<String[]> lineArrays = new ArrayList<>();
        for (String line : lines) {
            String[] lineArray = StringUtils.splitPreserveAllTokens(line, split);
        }

        List<String[]> failData = storeTable(dataBase, table, lineArrays);
        if (!CommonUtil.isEmpty(failData)) {
            FileUtil.writeFile(failPath, JSON.toJSONString(failData), false);
        }
        new File(filePath).delete();    //原文件删除
        storeTimeLogger.debug("load data to table[" + table + "] from path[" + filePath + "], time:" + (System.currentTimeMillis() - s));
    }

    /**
     * 保存表数据
     * 批量load
     * @param table
     * @param lines
     * @return
     * @throws Exception
     */
    public static List<String[]> storeTable(final String dataBase, final String table, List<String[]> lines) throws Exception {
        //记录失败的记录
        List<String[]> failList = new ArrayList<>();
        if (CommonUtil.isEmpty(lines)) {
            storeTimeLogger.error("[" + table + "] time:" + 0 + ", size:" + lines.size() + ", fail-size:" + failList.size());
            return failList;
        }
        long s = System.currentTimeMillis();

        TableInfo tbInfo = CacheUtil.readTableInfo(dataBase, table);
        if (tbInfo == null) {
            throw new BusinessException("unknow table: " + table);
        }
        //列定义
        ColumnDef[] columnDefs = tbInfo.getColumnDefs();
        //数据分列拆分结果
        List<List<Object>> dataList = new ArrayList<List<Object>>();
        Map<Integer, Long> longMap = new HashMap<Integer, Long>();
        /*String split = "!@#$%^&*()1234567890";  //避免单列随便给的初始值
        if (lines.size() > 0) {
            String ln = lines.get(0);
            if (ln.indexOf(Constants.COLUMN_SPLIT_2) > -1) {
                split = Constants.COLUMN_SPLIT_2;
            } else if (ln.indexOf(Constants.COLUMN_SPLIT_LEVEL_1) > -1) {
                split = Constants.COLUMN_SPLIT_LEVEL_1;
            }
        }*/
        for (int j = 0, m = lines.size(); j < m; j++) {
            //String line = lines.get(j);
            //String[] lineArray = StringUtils.splitPreserveAllTokens(line, split);
            String[] lineArray = lines.get(j);
            if (lineArray.length != columnDefs.length) {
                String failJson = JSON.toJSONString(lineArray);
                failList.add(lineArray);
                storeTimeLogger.error("the length of line is invalid, table:" + table + ", line:" + failJson);
                continue;
            }
            //先check一把，看看类型对不对
            boolean fail = false;
            //缓存结果避免二次 String to Long
            longMap.clear();
            for (int i = 0, k = columnDefs.length; i < k; i++) {
                if (!CommonUtil.isEmpty(lineArray[i])) {
                    if (columnDefs[i].isEnum()) {   //枚举校验加转换
                        Long l = getEnumIndex(dataBase, table, columnDefs[i].getName(), lineArray[i], columnDefs[i].getLength());
                        if (l == null) {
                            storeTimeLogger.error("data convert to enum fail, data:" + lineArray[i] + ", column:" + columnDefs[i]);
                            fail = true;
                            break;
                        }
                        longMap.put(i, l);
                    } else {    //非枚举类型，需要追加长度验证
                        if (columnDefs[i].getLength() > 0 && lineArray[i].length() > columnDefs[i].getLength()) {    //如果长度大于列长度，返回错误
                            storeTimeLogger.error("data length too more, data:" + lineArray[i] + ", column:" + columnDefs[i]);
                            fail = true;
                            break;
                        }
                        if (columnDefs[i].isLong()) {   //类型校验
                            Long l = CommonUtil.parseLong2(lineArray[i]);
                            if (l == null) {
                                storeTimeLogger.error("data convert to long fail, data:" + lineArray[i] + ", column:" + columnDefs[i]);
                                fail = true;
                                break;
                            }
                            longMap.put(i, l);
                        } else if (columnDefs[i].isDecimal()) {
                            BigDecimal f = CommonUtil.parseFloat2(lineArray[i]);
                            if (f == null) {
                                storeTimeLogger.error("data convert to float fail, data:" + lineArray[i] + ", column:" + columnDefs[i]);
                                fail = true;
                                break;
                            }
                            f = f.multiply(new BigDecimal(CommonUtil.pow10(columnDefs[i].getDecimalLength())));
                            Long l = f.longValue();
                            //Long l = CommonUtil.round((f * CommonUtil.pow10(columnDefs[i].getDecimalLength())));
                            longMap.put(i, l);
                        }
                    }

                }
            }
            if (fail) { //如果失败跳过，
                //String failJson = JSON.toJSONString(lineArray);
                failList.add(lineArray);
                storeTimeLogger.error("error line: " + lineArray);
                continue;
            }
            for (int i = 0, k = columnDefs.length; i < k; i++) {
                if (dataList.size() <= i) {
                    dataList.add(new ArrayList<Object>());
                }
                List<Object> col = dataList.get(i);
                if (columnDefs[i].isEnum() || columnDefs[i].isLong() || columnDefs[i].isDecimal()) {
                    col.add(longMap.get(i));
                } else {
                    col.add(lineArray[i]);
                }
            }
        }
        //long end = System.currentTimeMillis();
        //storeTimeLogger.debug("[" + table + "] do with data:" + (end - s));
        //s = end;
        if (CommonUtil.isEmpty(dataList)) {
            long time = System.currentTimeMillis() - s;
            storeTimeLogger.error("[" + table + "] time:" + time + ", size:" + lines.size() + ", fail-size:" + failList.size());
            return failList;
        }
        ReentrantLock lock = TableLockUtil.getLock(dataBase, table);    //加表锁
        lock.lock();
        try {
            //加锁之后，重新获取下表信息，
            tbInfo = CacheUtil.readTableInfo(dataBase, table);
            if (tbInfo == null) {
                throw new BusinessException("unknow table: " + table);
            }
            //先修改nextId，再进行数据插入，防止数据插入部分，nextId未更新的问题
            final long oldNextId = tbInfo.getNextId();
            final long nextId = oldNextId + lines.size() - failList.size();
            String key = DataBaseUtil.getTablePath(dataBase, table);
            String tableSeq = key + "/" + Constants.TABLE_SEQUENCE;
            FileUtil.writeFile(tableSeq, CommonUtil.asList(String.valueOf(nextId)), false);

            List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
            for (int i = 0, k = columnDefs.length; i < k; i++) {
                final List<Object> list = dataList.get(i);
                final ColumnDef def = columnDefs[i];
                if (def.isDecimal() || def.isEnum() || def.isLong()) {
                    Future<Boolean> f = PoolUtil.WRITE_FIX_POLL.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            return StoreManager.storeColumnL(dataBase, table, def, list);
                        }
                    });
                    futures.add(f);
                } else {
                    Future<Boolean> f = PoolUtil.WRITE_FIX_POLL.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            return StoreManager.storeColumnS(dataBase, table, def, list);
                        }
                    });
                    futures.add(f);
                }
            }
            boolean syncSuccess = true;
            for (Future<Boolean> f : futures) { //等待所有列都保存完毕
                Boolean res = f.get();
                if (res == null || !res) {
                    syncSuccess = false;
                }
            }
            final boolean syncDp = !syncSuccess;
            if (syncDp) {
                storeTimeLogger.warn("sync dp by bigstore failjps");
            }
            //删除表缓存
            //TableCache.removeTableCache(dataBase, table);
            //更新表缓存数据, 居然改动的只有nextId，那么将这个同步过去好了
            TableInfo tableInfo = CacheUtil.readTableInfo(dataBase, table);
            if (tableInfo != null) {
                tableInfo.setNextId(nextId);
            }
            //清理查询缓存
            int oldDirIndex = (int) (oldNextId / ServerConstants.PARK_SIZ);
            int nextDirIndex = (int) (nextId / ServerConstants.PARK_SIZ);
            while (oldDirIndex <= nextDirIndex) {
                DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
                QueryCache.clear(dataBase, table, null, df.format(oldDirIndex));
                oldDirIndex++;
            }
            //storeTimeLogger.debug("[" + table + "] write data by lock:" + (System.currentTimeMillis() - s));
            //如果有主从同步，则直接进行主从同步，
            if (Params.getSlaveIp() != null && Params.getCompressDelayHour() > 0) {
                PoolUtil.SEND_FIX_POLL.execute(new Runnable() {
                    @Override
                    public void run() {
                        int oldDirIndex = (int) (oldNextId / ServerConstants.PARK_SIZ);
                        int nextDirIndex = (int) (nextId / ServerConstants.PARK_SIZ);
                        while (oldDirIndex < nextDirIndex) {
                            DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
                            int dirSpaceIndex = oldDirIndex / Constants.SPACE_SIZ;
                            //进行数据同步
                            String dp = Params.getBaseDir() + dataBase + "/" + table + "/rows/" + df.format(dirSpaceIndex) + "/" + df.format(oldDirIndex);
                            for (int i = 0, k = 3; i < k; i++) {    //重试3次
                                String msg = ProcessUtil.executeScpSync(dp, Params.getSlaveIp(), syncDp);
                                if (msg != null && msg.startsWith("success:true")) {
                                    File sendFlag = new File(dp + "/" + FileConfig.SEND_FLAG);
                                    try {
                                        sendFlag.createNewFile();
                                        break;
                                    } catch (IOException e) {
                                        storeTimeLogger.error("create _senf.flg fail", e);
                                    }
                                } else {
                                    storeTimeLogger.warn("sync data fail, so don't create " + FileConfig.SEND_FLAG);
                                }
                            }
                            oldDirIndex++;
                        }
                    }
                });

            }
        } finally {
            lock.unlock();
        }
        long time = System.currentTimeMillis() - s;
        if (time <= 100) {
            storeTimeLogger.debug("[" + table + "] time:" + time + ", size:" + lines.size() + ", fail-size:" + failList.size());
        } else if (time <= 1000) {
            storeTimeLogger.info("[" + table + "] time:" + time + ", size:" + lines.size() + ", fail-size:" + failList.size());
        } else {
            storeTimeLogger.warn("[" + table + "] time:" + time + ", size:" + lines.size() + ", fail-size:" + failList.size());
        }

        return failList;
    }

    /**
     * 保存String型数据
     * @param table
     * @param columnDef
     * @param dataList
     */
    private static boolean storeColumnS(String dataBase, String table, ColumnDef columnDef, List<Object> dataList) throws Exception {
        boolean res = true;
        String col1File = DataBaseUtil.getTablePath(dataBase, table) + "/rows/";
        String column = columnDef.getName();
        List<String> lines = new ArrayList<String>();
        TableInfo tbInfo = CacheUtil.readTableInfo(dataBase, table);
        int dirIndex = (int) (tbInfo.getNextId() / ServerConstants.PARK_SIZ);
        long nextId = tbInfo.getNextId();   //倒排索引时，会使用
        int startInPark = (int) (nextId % ServerConstants.PARK_SIZ);
        boolean exsitData = startInPark > 0;    //当前块是否有数据的标识，
        int size = startInPark;
        int nullCount = 0;
        DecimalFormat df = new DecimalFormat("0000000000");
        for (int i = 0, k = dataList.size(); i < k; i++) {
            String time = (String) dataList.get(i);
            lines.add(time);
            size++;
            if (size == ServerConstants.PARK_SIZ || i == k - 1) { //
                int dirParentIndex = dirIndex / Constants.SPACE_SIZ;
                String space = df.format(dirParentIndex);
                String dirName = df.format(dirIndex);
                String spacePath = col1File + space;
                String dir = spacePath + "/" + dirName;
                new File(dir).mkdirs();
                String key = dir + "/" + column;
                if (exsitData) {    //如果当前块有数据，则获取块索引
                    IndexInfo indexInfo = CacheUtil.getIndexInfo(dataBase, table, space, dirName, column);
                    if (indexInfo != null) {
                        nullCount = indexInfo.getNullCount();
                    }
                }
                String data = key + FileConfig.DATA_FILE_SUFFIX;
                nullCount += FileUtil.writeData(data, lines, startInPark, true, columnDef.getLength(), false);
                String desc = key + FileConfig.DESC_FILE_SUFFIX;
                String bloonDesc = key + FileConfig.BOLLM_FILE_SUFFIX;
                final String indexDir = spacePath + "/" + column + FileConfig.INDEX_FILE_SUFFIX;
                //顺序参考 PackDescEnum
                List<String> datas = CommonUtil.asList(Constants.UN_PRESS, String.valueOf(lines.size() + startInPark), String.valueOf(nullCount), String.valueOf(Long.MIN_VALUE), String.valueOf(Long.MIN_VALUE));
                byte[] bytes = null;
                if (size == ServerConstants.PARK_SIZ) {
                    if (columnDef.isOrderString()) {   //如果是顺序列，则建立表分区索引
                        bytes = FileUtil.read2Byte(data);
                        List<String> linesOnRead = getStringLinesForStr(bytes);
                        BloomFilter bloomFilter = BloomUtil.createBlockBloom();
                        for (String s : linesOnRead) {
                            bloomFilter.addIfNotExist(s);
                        }
                        BloomUtil.writeBloom(bloomFilter, bloonDesc);
                        datas.add(FileConfig.BOLLM_TYPE_PATH + bloonDesc);
                    } else if (columnDef.isFindCol()) {
                        List<String> ddLines = null;
                        if (exsitData) {
                            //ddLines = FileUtil.readAll(data, false);
                            bytes = GZipUtil.readTxt2Byte(data);
                            ddLines = getLinesForStr(bytes);
                        } else {
                            ddLines = lines;
                        }
                        Map<Integer, String> asciiMap = new HashMap<Integer, String>();
                        for (String s : ddLines) {
                            byte[] bs = s.getBytes(Constants.DEFAULT_CHARSET);
                            for (int ii = 0, kk = Math.min(bs.length, Constants.STRING_INDEX_COUNT); ii < kk; ii++) {     //只取头32位作为索引
                                int ascii = bs[ii];
                                String index = asciiMap.get(ascii);
                                String currentIndex = FileConfig.INDEX_SPILT + ii + FileConfig.INDEX_SPILT;
                                if (index == null) {
                                    index = currentIndex;
                                    asciiMap.put(ascii, index);
                                } else {
                                    if (index.indexOf(currentIndex) == -1) {    //如果没找到，追加
                                        index += ii + FileConfig.INDEX_SPILT;
                                        asciiMap.put(ascii, index);
                                    }   //找到就算了，不处理了
                                }
                            }
                        }
                        for (Map.Entry<Integer, String> entry : asciiMap.entrySet()) {
                            datas.add(entry.getKey() + FileConfig.INDEX_FLAG + entry.getValue());
                        }
                    } else if (columnDef.isReverseIndex()) {
                        bytes = GZipUtil.readTxt2Byte(data);    //直接从硬盘读取吧，每次 都是全部新插入的 概率 比较小，就不做这个优化了
                        List<StringDataLine> dataLines = getLinesByStrForString(bytes);
                        SplitBuilder splitBuilder = new SplitBuilder();
                        Set<String> hashAll = new HashSet<String>(10000);
                        for (StringDataLine dataLine : dataLines) {
                            Set<String> tokenSet = splitBuilder.add(dataLine);
                            for (String s : tokenSet) {
                                hashAll.add(BusinessUtil.getHashCodeStr(s));
                            }
                        }
                        for (Map.Entry<Integer, List<String>> entry : splitBuilder.toLines().entrySet()) {
                            String file = key + FileConfig.SPLIT_WORD_SUFFIX.replace(FileConfig.SPLIT_WORD_IDX, String.valueOf(entry.getKey()));
                            FileUtil.writeFile(file, entry.getValue(), true);
                        }
                        //StringBuilder sb = new StringBuilder(FileConfig.INDEX_SPILT);
                        StringBuilder sb = new StringBuilder();
                        for (String s : hashAll) {
                            //sb.append(s).append(FileConfig.INDEX_SPILT);
                            sb.append(s);
                        }
                        datas.add(sb.toString());
                    }
                    //文件压缩，改为异步压缩
                    /*GZipUtil.compress(data);
                    new File(data).delete();
                    datas.set(PackDescEnum.PRESS.ordinal(), Constants.PRESS);*/
                }
                //判定是否需要建倒排索引
                /*if (columnDef.isReverseIndex()) {   //如果需要，建立倒排索引
                    final List<LuceneBean> lucDatas = new ArrayList<LuceneBean>();
                    int kk = lines.size();
                    for (int ii = 0; ii < kk; ii++) {
                        lucDatas.add(new LuceneBean(nextId + ii, lines.get(ii)));
                    }
                    PoolUtil.WRITE_FIX_POLL.submit(new Runnable() {
                        public void run() {
                            indexCount.incrementAndGet();
                            LuceneUtil.batchCreateIndex(indexDir, lucDatas);
                            indexCount.decrementAndGet();
                        }
                    });
                    nextId += kk;
                }*/
                FileUtil.writeFile(desc, datas, false);
                //重写之后，删除索引缓存
                TableCache.removeIndexCache(dataBase, table, dirName, column);
                //查询查询缓存
                QueryCache.clear(dataBase, table, column, dirName);
                //删除数据块DP缓存
                DataCache.remove(dataBase, table, dirName, column);

                if (SlaveClient.getSlaveClient() != null && size == ServerConstants.PARK_SIZ) { //如果是满块的做数据同步
                    if (bytes == null) {
                        bytes = GZipUtil.readTxt2Byte(data);
                    }
                    byte[] descData = GZipUtil.readTxt2Byte(desc);
                    //long ss = System.currentTimeMillis();
                    ResponseData response = SlaveClient.getSlaveClient().syncData(data, bytes);
                    if (response == null  || !response.isSuccess()) {
                        res = false;
                    }
                    response = SlaveClient.getSlaveClient().syncData(desc, descData);
                    if (response == null  || !response.isSuccess()) {
                        res = false;
                    }
                    //storeTimeLogger.debug("sync-send-file:" + (System.currentTimeMillis() - ss));
                }

                //字符串的分区索引构建
                if (size == ServerConstants.PARK_SIZ && dirName.endsWith(String.valueOf(Constants.SPACE_SIZ - 1))
                        && columnDef.isOrderString()) {
                    File spaceDir = new File(spacePath);
                    if (spaceDir.exists()) {
                        String spaceDesc = spacePath + "/" + column + FileConfig.SPACE_BLOOM_SUFFIX;
                        BloomFilter bloomFilter = BloomUtil.createSpaceBloom();
                        for (File dp : spaceDir.listFiles()) {
                            if (dp.isFile()) {
                                continue;
                            }
                            String colDataPath = dp.getPath() + "/" + column + FileConfig.DATA_FILE_SUFFIX;
                            byte[] bts = FileUtil.read2Byte(colDataPath);
                            List<String> linesOnRead = getStringLinesForStr(bts);
                            for (String ln : linesOnRead) {
                                bloomFilter.addIfNotExist(ln);
                            }
                        }
                        byte[] bt = KryoUtil.writeToByteArray(bloomFilter);
                        FileUtil.writeByte(spaceDesc, bt);
                    }

                }

                lines.clear();
                startInPark = 0;
                size = 0;
                nullCount = 0;
                dirIndex++;
                exsitData = false;
            }

        }
        return res;
    }

    /**
     * 保存Long型数据
     * @param table
     * @param columnDef
     * @param dataList
     */
    private static boolean storeColumnL(String dataBase, String table, ColumnDef columnDef, List<Object> dataList) throws Exception {
        boolean res = true;
        String col1File = DataBaseUtil.getTablePath(dataBase, table) + "/rows/";
        String column = columnDef.getName();
        List<Long> lines = new ArrayList<Long>();
        TableInfo tbInfo = CacheUtil.readTableInfo(dataBase, table);
        int dirIndex = (int) (tbInfo.getNextId() / ServerConstants.PARK_SIZ);
        int startInPark = (int) (tbInfo.getNextId() % ServerConstants.PARK_SIZ);
        boolean exsitData = startInPark > 0;    //当前块是否有数据的标识，
        int size = startInPark;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int nullCount = 0;
        BigDecimal sum = new BigDecimal(0);
        DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
        for (int i = 0, k = dataList.size(); i < k; i++) {
            Long time = (Long) dataList.get(i);
            if (time != null) { //如果不为空，计算最大最小值
                min = Math.min(min, time);
                max = Math.max(max, time);
                sum = sum.add(new BigDecimal(time));
            }
            lines.add(time);
            size++;
            if (size == ServerConstants.PARK_SIZ || i == k - 1) { //如果当前DP写满 或者 已经写到最后一行，则进行追加写入操作
                int dirSpaceIndex = dirIndex / Constants.SPACE_SIZ;
                String space = df.format(dirSpaceIndex);
                String dirName = df.format(dirIndex);
                String dir = col1File + space + "/" + dirName;
                File dirFile = new File(dir);
                dirFile.mkdirs();
                String key = dir + "/" + column;
                if (exsitData) {    //如果当前块有数据，则获取块索引
                    IndexInfo indexInfo = CacheUtil.getIndexInfo(dataBase, table, space, dirName, column);
                    if (indexInfo != null) {
                        if (indexInfo.getStart() > Long.MIN_VALUE && indexInfo.getStart() < Long.MAX_VALUE) {
                            min = Math.min(min, indexInfo.getStart());
                        }
                        if (indexInfo.getEnd() > Long.MIN_VALUE && indexInfo.getEnd() < Long.MAX_VALUE) {
                            max = Math.max(max, indexInfo.getEnd());
                        }
                        nullCount = indexInfo.getNullCount();
                        sum = sum.add(indexInfo.getSum()) ;
                    }
                }
                String data = key + FileConfig.DATA_FILE_SUFFIX;
                nullCount += FileUtil.writeData(data, lines, startInPark, true, columnDef.getLength(), true);
                if (min == Long.MAX_VALUE) {
                    min = max;
                }
                if (max == Long.MIN_VALUE) {
                    max = min;
                }
                String desc = key + FileConfig.DESC_FILE_SUFFIX;
                //顺序参考 PackDescEnum
                List<String> datas = CommonUtil.asList(Constants.UN_PRESS, String.valueOf(lines.size() + startInPark),
                        nullCount + FileConfig.NULL_SUM_SPLIT + sum.toString(), String.valueOf(min), String.valueOf(max));
                byte[] bytes = null;
                if (size == ServerConstants.PARK_SIZ) {
                    if (columnDef.isEnum()) {
                        List<LongDataLine> orderLines = null;
                        if (exsitData) {
                            bytes = GZipUtil.readTxt2Byte(data);
                            //List<String> ddLines = FileUtil.readAll(data, false);
                            orderLines = getOrderLinesByStrForEnum(bytes);
                        } else {
                            orderLines = getOrderLinesForEnum(lines);
                        }
                        List<LongCount> count = new ArrayList<LongCount>();
                        for (int ii = 0, kk = orderLines.size(); ii < kk; ii++) {
                            LongDataLine dataLine = orderLines.get(ii);
                            if (ii == 0) {
                                count.add(new LongCount(dataLine.getData(), dataLine.getNumber()));
                                continue;
                            }
                            LongCount last = count.get(count.size() - 1);
                            if (dataLine.getData() == last.getL()) {
                                last.addCount(dataLine.getNumber());
                            } else {
                                count.add(new LongCount(dataLine.getData(), dataLine.getNumber()));
                            }
                        }
                        for (LongCount ct : count) {
                            datas.add(ct.getL() + FileConfig.INDEX_FLAG + ct.getLineNums());
                        }
                    } else if (columnDef.isFindCol()) {
                        List<Long> orderLines = null;
                        if (exsitData) {
                            bytes = GZipUtil.readTxt2Byte(data);
                            //List<String> ddLines = FileUtil.readAll(data, false);
                            orderLines = getOrderLinesForStr(bytes);
                        } else {
                            orderLines = getOrderLines(lines);
                        }
                        List<IndexInfo.Index> indexList = calIndex(min, max, orderLines);
                        for (IndexInfo.Index idx : indexList) {
                            datas.add(idx.getStart() + FileConfig.INDEX_SPILT + idx.getEnd() + FileConfig.INDEX_FLAG + Constants.INDEX_EXSIT_INT);
                        }
                    }
                }
                FileUtil.writeFile(desc, datas, false);
                TableCache.removeIndexCache(dataBase, table, dirName, column);   //重写之后，删除索引缓存
                if (CommonManager.isOrderColumn(dataBase, table, column)) {   //如果是顺序列，则建立表分区索引
                    SpaceInfo spaceInfo = CacheUtil.getSpaceInfo(dataBase, table, dirSpaceIndex, column);
                    if (spaceInfo != null) {
                        min = Math.min(min, spaceInfo.getMin());
                        max = Math.max(max, spaceInfo.getMax());
                    }
                    //输出表分区数据到硬盘
                    String spaceKey = DataBaseUtil.getTablePath(dataBase, table) + "/rows/" + df.format(dirSpaceIndex) + "/" + column;
                    String spacePath = spaceKey + FileConfig.SPACE_FILE_SUFFIX;;
                    List<String> spaceData = CommonUtil.asList(String.valueOf(min), String.valueOf(max));
                    //如果是表分区的最后一个DP的最后一条数据已写满，则更新表分区索引
                    if (size == ServerConstants.PARK_SIZ && dirName.endsWith(String.valueOf(Constants.SPACE_SIZ - 1))) {
                        List<IndexInfo.Index> indexList = new ArrayList<>();
                        int last = CommonUtil.parseInt(dirName, 0);
                        for (int ii = 0; ii < Constants.SPACE_SIZ; ii++) {
                            int current = last - ii;
                            IndexInfo indexInfo = CacheUtil.getIndexInfo(dataBase, table, df.format(current), column);
                            if (indexInfo != null) {
                                indexList.addAll(indexInfo.getIndexes());
                            }
                        }
                        //索引合并
                        List<IndexInfo.Index> mergeIndex = MergeUtil.merge(indexList);
                        mergeIndex = MergeUtil.merge2small(mergeIndex);
                        for (IndexInfo.Index idx : mergeIndex) {
                            spaceData.add(idx.getStart() + FileConfig.INDEX_SPILT + idx.getEnd() + FileConfig.INDEX_FLAG + 1);
                        }
                    }
                    FileUtil.writeFile(spacePath, spaceData, false);
                    //删除表分区索引缓存
                    TableCache.removeSpaceCache(dataBase, table, df.format(dirSpaceIndex), column);
                }
                //查询查询缓存
                QueryCache.clear(dataBase, table, column, dirName);
                //删除数据块DP缓存
                DataCache.remove(dataBase, table, dirName, column);

                if (SlaveClient.getSlaveClient() != null && size == ServerConstants.PARK_SIZ) { //如果是满块的做数据同步
                    if (bytes == null) {
                        bytes = GZipUtil.readTxt2Byte(data);
                    }
                    byte[] descData = GZipUtil.readTxt2Byte(desc);
                    //long ss = System.currentTimeMillis();
                    ResponseData response = SlaveClient.getSlaveClient().syncData(data, bytes);
                    if (response == null  || !response.isSuccess()) {
                        res = false;
                    }
                    response = SlaveClient.getSlaveClient().syncData(desc, descData);
                    if (response == null  || !response.isSuccess()) {
                        res = false;
                    }
                    //storeTimeLogger.debug("sync-send-file:" + (System.currentTimeMillis() - ss));
                }

                //数据重置
                min = Long.MAX_VALUE;
                max = Long.MIN_VALUE;
                lines.clear();
                size = 0;
                startInPark = 0;
                nullCount = 0;
                sum = new BigDecimal(0);
                dirIndex++;
                exsitData = false;
            }
        }
        return res;
    }

    private static List<IndexInfo.Index> calIndex(long min, long max, List<Long> orderLines) {
        List<IndexInfo.Index> indexList = new ArrayList<>();
        if (min > 0 && max / min > 10) {
            //如果跨度太大可能是因为格式不一致，那么就需要收拢
            //多少个数据作为一段分区
            List<StartEnd> idxSpList = new ArrayList<>();
            int size = orderLines.size();
            long last = Long.MIN_VALUE;
            //头
            int lastIndex = 0;
            for (int i = 0; i < size; i++) {
                long l = orderLines.get(i);
                if(last > 0 && l / last >= 2)  {
                    idxSpList.add(new StartEnd(lastIndex , i - 1));
                    lastIndex = i;
                }
                last = l;
            }
            idxSpList.add(new StartEnd(lastIndex, size - 1));

            int spsize = idxSpList.size();
            //默认最多可以膨胀到2倍的区间数
            if (spsize > 0 && spsize < ServerConstants.PARK_INDEX_SIZE * 2) {
                int parkSubSize = ServerConstants.PARK_INDEX_SIZE / spsize;
                for (StartEnd idx : idxSpList) {
                    long minSub = orderLines.get(idx.getStart());
                    long maxSub = orderLines.get(idx.getEnd());
                    List<Long> orderSub = orderLines.subList(idx.getStart(), idx.getEnd() + 1);
                    List<IndexInfo.Index> indexListSub = calIndexSub(minSub, maxSub, orderSub, parkSubSize);
                    indexList.addAll(indexListSub);
                }
            } else {
                indexList = calIndexSub(min, max, orderLines, ServerConstants.PARK_INDEX_SIZE);
            }
        } else {
            indexList = calIndexSub(min, max, orderLines, ServerConstants.PARK_INDEX_SIZE);
        }
        //合并
        indexList = MergeUtil.merge(indexList);
        return indexList;
    }

    private static List<IndexInfo.Index> calIndexSub(long min, long max, List<Long> orderLines, int parkIndexSize) {
        List<IndexInfo.Index> indexList = new ArrayList<>();
        if (parkIndexSize == 0 || parkIndexSize == 1) {
            //如果不需要拆分，直接作为一个数值分区
            indexList.add(new IndexInfo.Index(min, max));
        } else {
            long onePart = Math.max((max - min) / parkIndexSize, 1L);
            List<IndexInfo.Index> spiltList = new ArrayList<>();
            for (long start = min; start <= max;) {
                long s = start;
                start += onePart;
                long end = Math.min(start, max);
                spiltList.add(new IndexInfo.Index(s, end));
            }
            for (IndexInfo.Index idx : spiltList) {
                if (isExsitInData(idx.getStart(), idx.getEnd(), orderLines) == Constants.INDEX_EXSIT_INT) {
                    indexList.add(idx);
                }
            }
        }
        return indexList;
    }

    private static List<LongDataLine> getOrderLinesByStrForEnum(byte[] bytes) {
        List<LongDataLine> lis = new ArrayList<LongDataLine>((int) ServerConstants.PARK_SIZ);
        int lineStart = 0;
        int lineEnd = 0;    // \n 的索引号
        int textStart = 0;  //分隔符的索引号
        for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == Constants.STACK_SPLIT) {
                textStart = i + 1;
            } else if (b == Constants.LINE_SEPARATOR) {
                lineEnd = i;

                long l = 0;
                int lineNum = 0;
                String dataStr = new String(bytes, textStart, lineEnd - textStart);
                String lineStr = new String(bytes, lineStart, textStart - 1 - lineStart);
                if (ServerConstants.USE_64) {
                    l = Convert10To64.unCompressNumberByLine(dataStr);
                    lineNum = (int) Convert10To64.unCompressNumberByLine(lineStr);
                } else {
                    l = CommonUtil.parseLong(dataStr);
                    lineNum = CommonUtil.parseInt(lineStr);
                }
                LongDataLine line = new LongDataLine(l, lineNum);
                lis.add(line);

                lineStart = i + 1;
            }


        }
        Collections.sort(lis);
        return lis;
    }

    /**
     * 获取原始数据的有序队列，并排除空
     * @param lines
     * @return
     */
    private static List<LongDataLine> getOrderLinesForEnum(List<Long> lines) {
        List<LongDataLine> orderLines = new ArrayList<LongDataLine>(lines.size() / 2);  //可能有空字符，所以其实申请一半大小的空间
        for (int i = 0, k = lines.size(); i < k; i++) {
            Long l = lines.get(i);
            if (l == null) {
                continue;
            }
            orderLines.add(new LongDataLine(l, i));
        }
        Collections.sort(orderLines);
        return orderLines;
    }

    /**
     * 获取原始数据的有序队列，并排除空
     * @param lines
     * @return
     */
    private static List<Long> getOrderLines(List<Long> lines) {
        List orderLines = new ArrayList(lines.size() / 2);  //可能有空字符，所以其实申请一半大小的空间
        for (Long l : lines) {
            if (l == null) {
                continue;
            }
            orderLines.add(l);
        }
        Collections.sort(orderLines);
        return orderLines;
    }

    /**
     * 获取原始数据的有序队列，并排除空
     * @param bytes
     * @return
     */
    private static List<Long> getOrderLinesForStr(byte[] bytes) {
        List orderLines = new ArrayList<Long>();
        int lineEnd = 0;    // \n 的索引号
        int textStart = 0;  //分隔符的索引号
        for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == Constants.STACK_SPLIT) {
                textStart = i + 1;
            } else if (b == Constants.LINE_SEPARATOR) {
                lineEnd = i;

                long l = 0;
                int lineNum = 0;
                String dataStr = new String(bytes, textStart, lineEnd - textStart);
                if (ServerConstants.USE_64) {
                    l = Convert10To64.unCompressNumberByLine(dataStr);
                } else {
                    l = CommonUtil.parseLong(dataStr);
                }
                orderLines.add(l);
            }


        }
        Collections.sort(orderLines);
        return orderLines;
    }

    /**
     * 获取原始数据的字符串数据，并排除空
     * @param bytes
     * @return
     */
    private static List<String> getStringLinesForStr(byte[] bytes) {
        List strLines = new ArrayList<String>();
        if (bytes == null) {
            return strLines;
        }
        int lineEnd = 0;    // \n 的索引号
        int textStart = 0;  //分隔符的索引号
        for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == Constants.STACK_SPLIT) {
                textStart = i + 1;
            } else if (b == Constants.LINE_SEPARATOR) {
                lineEnd = i;
                String dataStr = new String(bytes, textStart, lineEnd - textStart);
                strLines.add(dataStr);
            }
        }
        return strLines;
    }

    /**
     * 判定当前数据，在次区间是否存在
     * @param start
     * @param end
     * @param indexList
     * @return
     */
    private static int isExsitInDataByIndex(long start, long end, List<IndexInfo> indexList) {
        for (IndexInfo idx : indexList) {
            //有交叉就判定是存在的
            if (end >= idx.getStart() || start <= idx.getEnd()) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * 判定当前数据，在次区间是否存在
     * @param start
     * @param end
     * @param datas
     * @return
     */
    private static int isExsitInData(long start, long end, List<Long> datas) {
        int low = 0;
        int high = datas.size() - 1;
        while (low <= high) {
            int middle = (low + high) / 2;
            long l = datas.get(middle);
            if (l > end) {
                high = middle - 1;
            } else if (l < start) {
                low = middle + 1;
            } else {
                return 1;
            }
        }
        return 0;
    }

    private static List<StringDataLine> getLinesByStrForString(byte[] bytes) {
        List<StringDataLine> lis = new ArrayList<StringDataLine>((int) ServerConstants.PARK_SIZ);
        int lineStart = 0;
        int lineEnd = 0;    // \n 的索引号
        int textStart = 0;  //分隔符的索引号
        for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == Constants.STACK_SPLIT) {
                textStart = i + 1;
            } else if (b == Constants.LINE_SEPARATOR) {
                lineEnd = i;

                long l = 0;
                int lineNum = 0;
                String dataStr = new String(bytes, textStart, lineEnd - textStart);
                String lineStr = new String(bytes, lineStart, textStart - 1 - lineStart);
                if (ServerConstants.USE_64) {
                    lineNum = (int) Convert10To64.unCompressNumberByLine(lineStr);
                } else {
                    lineNum = CommonUtil.parseInt(lineStr);
                }
                StringDataLine line = new StringDataLine(dataStr, lineNum);
                lis.add(line);

                lineStart = i + 1;
            }


        }
        return lis;
    }

    private static List<String> getLinesForStr(byte[] bytes) {
        List<String> result = new ArrayList<String>((int) ServerConstants.PARK_SIZ);
        int lineEnd = 0;    // \n 的索引号
        int textStart = 0;  //分隔符的索引号
        for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == Constants.STACK_SPLIT || b == '\0') {
                textStart = i + 1;
            } else if (b == Constants.LINE_SEPARATOR) {
                lineEnd = i;
                result.add(new String(bytes, textStart, lineEnd - textStart));
            }
        }
        return result;
    }

    private static Long getEnumIndex(String dataBase, String table, String col, String data, int length) throws Exception {
        EnumInfo enumInfo = CacheUtil.getEnumInfo(dataBase, table, col);
        if (enumInfo == null) {
            enumInfo = new EnumInfo();
        }
        Long l = enumInfo.getIndex(data);
        int next = enumInfo.size() + 1;
        if (l == null && (length == 0 || String.valueOf(next).length() <= length)) {
            l = enumInfo.addEnum(data);
            String colEnumPath = DataBaseUtil.getTablePath(dataBase, table) + "/" + col + Constants.COL_ENUM_TXT;
            FileUtil.writeFile(colEnumPath, enumInfo.toTextLines(), false);
        }
        return l;
    }

    public static void main(String[] args) {
        /*Random ran = new Random();
        List<Long> list = new ArrayList<>();
        int bei = 1;
        for (int i = 0 ; i < 10000; i++) {
            //long l = 20L + i + ran.nextInt(100);
            long l = 20L + i;
            if (i % 1000 == 0) {
                bei *= 3;
            }
            list.add(l * bei);
        }

        Collections.sort(list);
        StoreManager.calIndex(2022, 20220305, list);*/

        /*BigDecimal f = CommonUtil.parseFloat2("524113310.38");
        //Float f = CommonUtil.parseFloat2("5.6");
        f = f.multiply(new BigDecimal(CommonUtil.pow10(2)));
        Long l = f.longValue();
        System.out.println(l);*/

        int length = 2;
        BigDecimal d = BigDecimal.valueOf(52411331030L).divide(BigDecimal.valueOf(CommonUtil.pow10(length)));
        d.setScale(length);
        String l = d.toString();
        System.out.println(l);
    }

}
