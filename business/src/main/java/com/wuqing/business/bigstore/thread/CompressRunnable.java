package com.wuqing.business.bigstore.thread;

import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.PressConstants;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.process.ProcessUtil;
import com.wuqing.business.bigstore.util.*;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.config.PackDescEnum;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import com.wuqing.business.bigstore.util.CacheUtil;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.business.bigstore.util.GZipUtil;
import com.wuqing.business.bigstore.util.PoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by wuqing on 17/6/23.
 */
public class CompressRunnable implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger("compress-log");

    private String baseDir;

    private static final int MAX_LEVEL = 7;

    //扫描最近时间范围内，发生变更的表
    private static final long SCAN_INCLUDE_TIME = Params.getScanTableHour() * 3600 * 1000;

    //扫描最近时间范围内，发生变更的DS,DP
    private static final long SCAN_DP_INCLUDE_TIME = Params.getScanDpHour() * 3600 * 1000;

    //同步check变化，时间范围内的文件，例如 默认只check 1 小时内有变化的文件，如果1小时未变化，则设置标记，永远不check，也不进行同步
    private static final long SYNCHRONIZATION_WAIT_TIME = Params.getSendWaitHour() * 3600 * 1000;

    private static final long FORCE_SEND_WAIT_TIME = Params.getForceSendMinute() * 60 * 1000;   //默认20分钟

    //压缩延迟时间
    private static final long COMPRESS_DELAY_TIME = (long) (Params.getCompressDelayHour() * 3600 * 1000);

    //并发许可长度
    private static final int SEMAP_SIZE = Params.getCompressThreadSize() * 2;

    public CompressRunnable(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (PressConstants.COMPRESS_TYPE == PressConstants.NO_PRESS_TYPE) {
                    logger.info("no press");
                    return;
                }
                logger.info("start to compress data");
                File file = new File(this.baseDir);
                compress(file, 0);
                logger.info("end to compress data");
                waitRelease();
                Thread.sleep(5000);   //休息5秒
            } catch (Exception e) {
                logger.error("compress fail.", e);
            }
        }
    }

    private static void waitRelease() {
        try {
            PoolUtil.COMPRESS_SEMAP.acquire(SEMAP_SIZE);
        } catch (InterruptedException e) {
        } finally {
            PoolUtil.COMPRESS_SEMAP.release(SEMAP_SIZE);
        }
    }

    private void compress(File file, int level) throws InterruptedException {
        if (level > MAX_LEVEL) {
            throw new RuntimeException("compress level is more than " + MAX_LEVEL);
        }
        if (!file.isDirectory()) {
            return;
        }
        if (level > 1) {    //0:base目录, 1:database
            if (level == 2) {   //2:table
                if (SCAN_INCLUDE_TIME > 0 && System.currentTimeMillis() - file.lastModified() > SCAN_INCLUDE_TIME) {
                    return;
                }
            } else if (level >= 4) {    //4:DS, 5:DP
                if (SCAN_DP_INCLUDE_TIME > 0 && System.currentTimeMillis() - file.lastModified() > SCAN_DP_INCLUDE_TIME) {
                    return;
                }
            }
        }
        level++;    //递归层数加1
        for (final File f : file.listFiles()) {
            if (f.isDirectory()) {
                if (level == 5) {   //最底层的文件目录
                    PoolUtil.COMPRESS_SEMAP.acquire();
                    PoolUtil.COMPRESS_FIX_POLL.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                long last = f.lastModified();
                                if (last < 1503936000000L) {    //小于8月29日0点的不处理
                                    return;
                                }
                                File[] childs = f.listFiles();
                                if (childs == null || childs.length == 0) {
                                    return;
                                }
                                compressData(f, childs);
                                /*long lastTime = sendData(f, childs);
                                cacheIndex(f, childs, lastTime);*/
                                //去除延迟发送数据，只用时时同步，提高IO
                                //cacheIndex(f, childs, 0);
                            } catch (Exception e) {
                                logger.error("compress file fail. filePath:" + f.getPath(), e);
                            } finally {
                                PoolUtil.COMPRESS_SEMAP.release();
                            }
                        }
                    });
                } else {
                    compress(f, level);
                }
                if (level == 4) {   //数据压缩
                    final File[] childs = f.listFiles();
                    try {   //使用主线程进行文件合并，避免把IO打满
                        //数据已经是合并的了，后面要做的可能只是压缩而已
                        //mergerData(f, childs);
                    } catch (Exception e) {
                        logger.error("merger data fail", e);
                    }
                    PoolUtil.COMPRESS_SEMAP.acquire();
                    PoolUtil.COMPRESS_FIX_POLL.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                compressMergerData(f, childs);
                            } catch (Exception e) {
                                logger.error("compress merger data fail, path:" + f.getPath());
                            } finally {
                                PoolUtil.COMPRESS_SEMAP.release();
                            }
                        }
                    });
                }
            }
        }
    }

    private void mergerData(File f, File[] childs) throws Exception {
        if (!Params.isMergerData()) {   //需要进行数据合并的话
            return;
        }
        File mergerFlag = new File (f.getPath() + "/" + FileConfig.MERGER_FLAG);
        if (mergerFlag.exists()) {
            return;
        }
        Integer space = CommonUtil.parseInt2(f.getName());
        if (space == null) {
            return;
        }
        int last = space * Constants.SPACE_SIZ + Constants.SPACE_SIZ - 1;
        DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
        File lastFile = new File(f.getPath() + "/" + df.format(last));
        if (!lastFile.exists()) {
            return;
        }
        boolean full = true;
        for (File data : lastFile.listFiles()) {
            if (data.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                String total = FileUtil.readLine(data, PackDescEnum.COUNT.ordinal());
                full = String.valueOf(ServerConstants.PARK_SIZ).equals(total);
                if (!full) {
                    return; //未满，不进行数据合并
                }
            }
        }
        Map<String, List<File>> dataGroup = new HashMap<String, List<File>>();
        int length = 0;
        int dataLength = 0;
        boolean sendFlag = true;
        //boolean cacheFlag = true;
        //Set<String> dpSet = new HashSet<String>();
        for (File dp : childs) {
            if (dp.isFile()) {
                continue;
            }
            //dpSet.add(dp.getPath());
            int leng = 0;
            int dataLen = 0;
            boolean dpSendFlag = false;
            //boolean dpCacheFlag = false;
            for (File data : dp.listFiles()) {
                String name = data.getName();
                if (name.endsWith(FileConfig.SEND_FLAG)) {
                    dpSendFlag = true;
                    continue;
                }
                /*if (name.endsWith(FileConfig.CACHE_FLAG)) {
                    dpCacheFlag = true;
                    continue;
                }*/
                if (!name.endsWith(FileConfig.DATA_INDEX_SUBFFIX)) {
                    continue;   //如果不是 .txt结尾的，不处理跳过
                } else {
                    dataLen++;
                }
                leng++;
                List<File> fileList = dataGroup.get(name);
                if (fileList == null) {
                    fileList = new ArrayList<File>();
                    dataGroup.put(name, fileList);
                }
                fileList.add(data);
            }
            sendFlag &= dpSendFlag;
            //cacheFlag &= dpCacheFlag;
            if (length == 0) {
                length = leng;
            } else {
                if (length != leng) {
                    logger.warn("count of data and index is not eq, space:" + f.getPath());
                    return; //如果目录的文件数量都不一直直接返回，不合并
                }
            }
            if (dataLength == 0) {
                dataLength = dataLen;
            } else {
                if (dataLength != dataLen) {
                    logger.warn("count of data is not eq, space:" + f.getPath());
                    return; //如果目录的文件数量都不一直直接返回，不合并
                }
            }
        }
        if (length == 0 || dataLength == 0) {
            //logger.warn(" data length is 0, space:" + f.getPath());
            return;
        }
        if (Params.getSlaveIp() != null && !sendFlag) {
            return;
        }
        /*if (Params.isLoadIndex() && !cacheFlag) {
            return;
        }*/
        for (Map.Entry<String, List<File>> entry : dataGroup.entrySet()) {
            File mergerFile = new File(f.getPath() +  "/" + entry.getKey());
            //mergerFile.deleteOnExit();
            BufferedOutputStream bufferedStream = null;
            FileOutputStream fileStream = null;
            try {
                fileStream = new FileOutputStream(mergerFile);
                bufferedStream = new BufferedOutputStream(fileStream);
                byte[] bytesAll = null;
                for (File dd : entry.getValue()) {
                    byte[] ddByte = GZipUtil.readTxt2Byte(dd);
                    int byLenth = ServerConstants.DIR_FORMAT.length() + 3 + ddByte.length;
                    byte[] packBytes = new byte[byLenth];
                    int count = 0;
                    //bufferedStream.write(ServerConstants.DIR_FLAG_START);
                    packBytes[count] = ServerConstants.DIR_FLAG_START;
                    count++;
                    //bufferedStream.write(dd.getParentFile().getName().getBytes());
                    byte[] dirNameBytes = dd.getParentFile().getName().getBytes();
                    System.arraycopy(dirNameBytes, 0, packBytes, count, dirNameBytes.length);
                    count += ServerConstants.DIR_FORMAT.length();
                    //bufferedStream.write(ServerConstants.DIR_FLAG_END);
                    packBytes[count] = ServerConstants.DIR_FLAG_END;
                    count++;
                    //bufferedStream.write(ServerConstants.LINE_SEPARATOR);
                    packBytes[count] = ServerConstants.LINE_SEPARATOR;
                    count++;
                    //bufferedStream.write();  //写入压缩后的数据
                    System.arraycopy(ddByte, 0, packBytes, count, ddByte.length);
                    if (Params.isLoadIndex() && dd.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                        CacheUtil.createIndexInfo(dd.getPath(), ddByte);
                    }
                    if (bytesAll == null) {
                        bytesAll = packBytes;
                    } else {
                        byte[] newByte = new byte[bytesAll.length + packBytes.length];
                        System.arraycopy(bytesAll, 0, newByte, 0, bytesAll.length);
                        System.arraycopy(packBytes, 0, newByte, bytesAll.length, packBytes.length);
                        bytesAll = newByte;
                    }
                }
                bufferedStream.write(SnappyUtil.compress(bytesAll));
                bufferedStream.flush();
            } finally {
                CommonUtil.close(bufferedStream);
                CommonUtil.close(fileStream);
            }
        }
        //先创建标记文件，说明文件已合并完成
        mergerFlag.createNewFile();
        //剩下的就是删除原始数据了
        ProcessUtil.executeDelDataFile(f.getPath());
        /*for (String dpPath : dpSet) {
            ProcessUtil.executeDelDataFile(dpPath);
        }*/
        /*for (Map.Entry<String, List<File>> entry : dataGroup.entrySet()) {
            for (File dd : entry.getValue()) {
                logger.debug("delte data:" + dd.getPath());
                dd.delete();
            }
        }*/
        logger.debug("merger data:" + f.getPath());
    }

    /**
     * 同步数据文件，目前由于DP块在写满时已经做了，主从同步了，所以这里就没有再启用主从同步
     * 不过为了防止有DP块永远不写满，或者过了很久才写满的情况下，导致数据主从延迟过大，这里其实还是要需要check的
     * @param f
     * @param childs
     * @return
     * @throws Exception
     */
    private long sendData(File f, File[] childs) throws Exception {
        if (Params.getSlaveIp() == null) {
            return 0L;
        }
        File sendFlag = new File(f.getPath() + "/" + FileConfig.SEND_FLAG);
        if (sendFlag.exists()) {    //如果已发送标记存在，则直接返回
            return 0L;
        }
        long lastTime = 0L;
        Boolean full = null;    //是否是一个满DP块
        for (File fff : childs) {
            if (fff.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                lastTime = Math.max(lastTime, fff.lastModified());
                if (full == null) {
                    String total = FileUtil.readLine(fff, PackDescEnum.COUNT.ordinal());
                    full = String.valueOf(ServerConstants.PARK_SIZ).equals(total);
                }
            }
        }
        if (lastTime == 0L || full == null) {
            return 0L;
        }
        if (!full && System.currentTimeMillis() - lastTime < FORCE_SEND_WAIT_TIME) {    //如果没满的情况下，最近一段时间内有便跟，则不通同步
            return lastTime;
        }
        File lastFlag = new File(f.getPath() + "/" + lastTime + FileConfig.SEND_FLAG);
        if (lastFlag.exists()) {
            if (SYNCHRONIZATION_WAIT_TIME > 0 && System.currentTimeMillis() - lastTime > SYNCHRONIZATION_WAIT_TIME) {
                sendFlag.createNewFile();   //如果1小时内，没发送变动，则此文件将不在check变动，也不再主从同步
            }
            return lastTime; //发送过了，
        }
        //数据主从同步
        String msg = ProcessUtil.executeScpSync(f.getPath(), Params.getSlaveIp(), true);
        if (msg != null && msg.startsWith("success:true")) {
            lastFlag.createNewFile();
        } else {
            logger.warn("sync data fail, so don't create " + FileConfig.SEND_FLAG);
        }
        return lastTime;
    }

    /**
     * 加载索引文件缓存
     * @param f
     * @throws Exception
     */
    private static void cacheIndex(File f, File[] childs, long lastTime) throws Exception {
        if (!Params.isLoadIndex()) {
            return;
        }
        File cacheFlag = new File(f.getPath() + "/" + FileConfig.CACHE_FLAG);
        if (cacheFlag.exists()) {
            return;
        }
        if (lastTime == 0L) {
            for (File fff : childs) {
                if (fff.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                    lastTime = Math.max(lastTime, fff.lastModified());
                }
            }
        }
        if (lastTime == 0) {    //如果当前目录下没有索引文件，则直接结束
            return;
        }
        File lastFlag = new File(f.getPath() + "/" + lastTime + FileConfig.CACHE_FLAG);
        if (lastFlag.exists()) {
            if (SYNCHRONIZATION_WAIT_TIME > 0 && System.currentTimeMillis() - lastTime > SYNCHRONIZATION_WAIT_TIME) {
                cacheFlag.createNewFile();
            }
            return;
        }
        boolean index = false;
        for (File fff : childs) {
            if (fff.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                CacheUtil.createIndexInfo(fff.getPath(), null);
                index = true;
            }
        }
        if (index) {
            logger.debug("cache-index: " + f.getPath());
            lastFlag.createNewFile();
        }
    }

    private static void compressMergerData(File f, File[] childs) throws Exception {
        if ((System.currentTimeMillis() - f.lastModified()) < COMPRESS_DELAY_TIME) { //大于3天进行压缩, 也就是说3天之后才进行压缩
            return;
        }
        File mergerFlag = new File (f.getPath() + "/" + FileConfig.MERGER_FLAG);
        if (!mergerFlag.exists()) { //如果合并标识不存在，直接结束
            return;
        }
        File compressFlag = new File(f.getPath() + "/" + FileConfig.COMPRESS_FLAG);
        if (compressFlag.exists()) {    //如果压缩标记存在，则直接结束
            return;
        }
        //开始进行数据压缩
        boolean haveTxtData = false;    //默认没有txt数据
        for (File fff : childs) {
            if (fff.isDirectory()) {
                continue;
            }
            if (fff.getName().endsWith(FileConfig.DATA_FILE_SUFFIX)) {
                haveTxtData = true;
                break;
            }
        }
        if (!haveTxtData) {
            return;
        }

        for (File fff : childs) {
            if (fff.isDirectory()) {
                continue;
            }
            String dataName = fff.getPath();
            if (!dataName.endsWith(FileConfig.DATA_FILE_SUFFIX)) { //非数据文件跳过
                continue;
            }
            String col = dataName.substring(0, dataName.length() - FileConfig.DATA_FILE_SUFFIX.length());
            String descName = col + FileConfig.DESC_FILE_SUFFIX;
            GZipUtil.compress(dataName, true);
            //读取索引文件
            byte[] indexBytes = GZipUtil.read2Byte(descName);
            if (indexBytes == null) {
                logger.error("merger index file is not exsit, index:" + descName);
                continue;
            }
            byte[] uncompres = SnappyUtil.decompressWithCheck(indexBytes);
            if (uncompres != null) {
                indexBytes = uncompres;
            }
            int dirStart = 0;
            Set<String> dirSet = new HashSet<String>();
            String dirInMerger = null;
            for (int i = 0, k = indexBytes.length; i < k; i++) {
                byte b = indexBytes[i];
                if (b == ServerConstants.DIR_FLAG_START) {
                    dirStart = i + 1;
                } else if (b == ServerConstants.DIR_FLAG_END) {
                    indexBytes[i + 2] = PressConstants.COMPRESS_TYPE_BYTE;  //还有个换行符
                    dirInMerger = new String(indexBytes, dirStart, ServerConstants.DIR_LENGTH);
                    dirSet.add(dirInMerger);
                }
            }
            //写入索引文件
            FileUtil.writeByte(new File(descName), SnappyUtil.compress(indexBytes));
            String file = dataName.substring(Params.getBaseDir().length());
            //更新索引缓存
            String[] dirs = file.split("/");
            col = col.substring(col.lastIndexOf("/") + 1);
            for (String dir : dirSet) {
                IndexInfo indexInfo = TableCache.getIndex(dirs[0], dirs[1], dir, col);
                if (indexInfo != null) {
                    indexInfo.setPress(PressConstants.COMPRESS_TYPE);
                }
            }
            //索引写入，缓存更新之后，再执行文件删除
            new File(dataName).delete();
        }
        logger.debug("compress " + f.getPath());
        compressFlag.createNewFile();
    }
    /**
     * 压缩数据文件
     * @param f
     * @throws Exception
     */
    private static void compressData(File f, File[] childs) throws Exception {
        if ((System.currentTimeMillis() - f.lastModified()) < COMPRESS_DELAY_TIME) { //大于3天进行压缩, 也就是说3天之后才进行压缩
            return;
        }
        /*if (Params.isMergerData()) {    //如果做数据合并的话，将不会在DP层进行数据压缩
            return;
        }*/
        File compressFlag = new File(f.getPath() + "/" + FileConfig.COMPRESS_FLAG);
        if (compressFlag.exists()) {
            return;
        }
        //开始进行数据压缩
        boolean haveTxtData = false;    //默认没有txt数据
        boolean fullDataPack = true;    //数据已满，可以进行压缩
        for (File fff : childs) {
            if (fff.getName().endsWith(FileConfig.DATA_FILE_SUFFIX)) {
                haveTxtData = true;
                break;
            }
        }
        if (!haveTxtData) {
            return;
        }
        int descCount = 0;
        for (File fff : childs) {
            if (fff.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                descCount++;
                String total = FileUtil.readLine(fff, PackDescEnum.COUNT.ordinal());
                if (!String.valueOf(ServerConstants.PARK_SIZ).equals(total)) { //如果不是满块的，跳过
                    fullDataPack = false;   //数据没满
                    break;
                }
            }
        }
        if (!fullDataPack || descCount == 0) {  //DP块数据没满，直接诶返回

            return;
        }
        for (File fff : childs) {
            String dataName = fff.getPath();
            if (!dataName.endsWith(FileConfig.DATA_FILE_SUFFIX)) { //非数据文件跳过
                continue;
            }
            String col = dataName.substring(0, dataName.length() - FileConfig.DATA_FILE_SUFFIX.length());
            String descName = col + FileConfig.DESC_FILE_SUFFIX;
            GZipUtil.compress(dataName);
            //写索引文件
            byte[] bytes = GZipUtil.read2Byte(descName);
            if (bytes != null) {
                bytes[0] = PressConstants.COMPRESS_TYPE_BYTE;
                FileUtil.writeByte(new File(descName), SnappyUtil.compress(bytes));
            }
            //FileUtil.writeLine(descName, String.valueOf(PressConstants.COMPRESS_TYPE), PackDescEnum.PRESS.ordinal());
            String file = dataName.substring(Params.getBaseDir().length());
            //更新索引缓存
            String[] dirs = file.split("/");
            col = col.substring(col.lastIndexOf("/") + 1);
            //压缩时加载index到缓存
            //IndexInfo indexInfo = CacheUtil.getIndexInfo(dirs[0], dirs[1], dirs[4], col);
            //压缩时不加载index到缓存
            IndexInfo indexInfo = TableCache.getIndex(dirs[0], dirs[1], dirs[4], col);
            if (indexInfo != null) {
                indexInfo.setPress(PressConstants.COMPRESS_TYPE);
            }
            //索引写入，缓存更新之后，再执行文件删除
            new File(dataName).delete();
        }
        logger.debug("compress " + f.getPath());
        compressFlag.createNewFile();
    }

    public static void start() {
        Runnable run = new CompressRunnable(Params.getBaseDir());
        PoolUtil.CACHED_POLL.execute(run);
    }

    public static void main(String[] args) throws InterruptedException {
        start();
        Thread.sleep(1000 * 1000L);
    }

}
