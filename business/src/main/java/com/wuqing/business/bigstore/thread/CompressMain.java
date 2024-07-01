package com.wuqing.business.bigstore.thread;

import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.PressConstants;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.business.bigstore.util.GZipUtil;
import com.wuqing.business.bigstore.util.PoolUtil;
import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.config.PackDescEnum;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by wuqing on 17/6/23.
 */
public class CompressMain {

    private final static Logger logger = LoggerFactory.getLogger("compress-log");

    private String baseDir;

    private static final int MAX_LEVEL = 7;

    private static final int SEMAP_SIZE = Params.getCompressThreadSize() * 2;

    public CompressMain(String baseDir) {
        this.baseDir = baseDir;
    }

    private static void compress(File file, int level) throws InterruptedException {
        if (level > MAX_LEVEL) {
            throw new RuntimeException("compress level is more than " + MAX_LEVEL);
        }
        if (!file.isDirectory()) {
            return;
        }
        level++;    //递归层数加1
        for (final File f : file.listFiles()) {
            System.out.print("file:" + f.getPath() + ",level:" + level);
            if (f.isDirectory()) {
                if (level == 5) {   //最底层的文件目录
                    PoolUtil.COMPRESS_SEMAP.acquire();
                    PoolUtil.COMPRESS_FIX_POLL.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                boolean haveTxtData = false;    //默认没有txt数据
                                boolean fullDataPack = true;    //数据已满，可以进行压缩
                                long totalTime = 0;
                                long lastTime = 0;
                                for (File fff : f.listFiles()) {
                                    if (fff.getName().endsWith(FileConfig.DATA_FILE_SUFFIX)) {
                                        haveTxtData = true;
                                        break;
                                    }
                                    totalTime += fff.lastModified();
                                    lastTime = Math.max(lastTime, fff.lastModified());
                                }
                                if (!haveTxtData) {
                                    return;
                                }
                                for (File fff : f.listFiles()) {
                                    if (fff.getName().endsWith(FileConfig.DESC_FILE_SUFFIX)) {
                                        String total = FileUtil.readLine(fff, PackDescEnum.COUNT.ordinal());
                                        if (!String.valueOf(ServerConstants.PARK_SIZ).equals(total)) { //如果不是满块的，跳过
                                            fullDataPack = false;   //数据没满
                                            break;
                                        }
                                    }
                                }
                                if (!fullDataPack) {  //DP块数据没满，直接诶返回
                                    return;
                                }
                                for (File fff : f.listFiles()) {
                                    String dataName = fff.getPath();
                                    if (!dataName.endsWith(FileConfig.DATA_FILE_SUFFIX)) { //非数据文件跳过
                                        continue;
                                    }
                                    String col = dataName.substring(0, dataName.length() - FileConfig.DATA_FILE_SUFFIX.length());
                                    String descName = col + FileConfig.DESC_FILE_SUFFIX;
                                    GZipUtil.compress(dataName);
                                    //写索引文件
                                    FileUtil.writeLine(descName, String.valueOf(PressConstants.COMPRESS_TYPE), PackDescEnum.PRESS.ordinal());
                                    String file = dataName.substring(Params.getBaseDir().length());
                                    //更新索引缓存
                                    String[] dirs = file.split("/");
                                    col = col.substring(col.lastIndexOf("/") + 1);
                                    //压缩时候加载index到缓存
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

    public static void main(String[] args) throws InterruptedException {
        String dir = "/tmp/bigstore/default_data_base/test_table";
        int level = 2;
        if (args.length == 1) {
            dir = args[0];
        } else if (args.length >= 2) {
            dir = args[0];
            level = CommonUtil.parseInt(args[1]);
        }
        System.out.println("dir:" + dir);
        System.out.println("level:" + level);
        System.out.println("start");
        compress(new File(dir), level);
        System.out.println("end");
        waitRelease();
        System.exit(0);
    }

}
