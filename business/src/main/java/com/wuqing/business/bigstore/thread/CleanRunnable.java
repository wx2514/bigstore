package com.wuqing.business.bigstore.thread;

import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.util.PoolUtil;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wx2514 on 2020/5/11.
 */
public class CleanRunnable implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger("clean-log");

    /**
     * base目录
     */
    private String baseDir;

    /**
     * 超过 cleanTableNeverChangeMillis 没改变的 表 将会被清理(毫秒)
     */
    private long cleanTableNeverChangeMillis;

    /**
     * 超过 cleanTableNeverChangeMillis 没改变的 分区 将会被清理(毫秒)
     */
    private long cleanSpaceNeverChangeMillis;

    /**
     * 超过 cleanTableNeverChangeMillis 没改变的 分区 将会被清理(毫秒)
     */
    private Map<String, Long> cleanSpaceNeverChangeMillisByDbMap = new HashMap<>();

    /**
     * 超过 cleanTableNeverChangeMillis 没改变的 分区 将会被清理(毫秒)
     */
    private Map<String, Long> cleanSpaceNeverChangeMillisByTableMap = new HashMap<>();

    private static final List<String> CLEAN_EXCLUDE_TABLES = Params.getCleanExcludeTables();

    public CleanRunnable(String baseDir, float cleanTableNeverChangeDay, float cleanSpaceNeverChangeDay,
                         String cleanSpaceNeverChangeDayByDb, String cleanSpaceNeverChangeDayByTable) {
        this.baseDir = baseDir;
        this.cleanTableNeverChangeMillis = (long) (cleanTableNeverChangeDay * 24 * 3600 * 1000);
        this.cleanSpaceNeverChangeMillis = (long) (cleanSpaceNeverChangeDay * 24 * 3600 * 1000);
        //DB级别的清理策略
        if (!CommonUtil.isEmpty(cleanSpaceNeverChangeDayByDb)) {
            cleanSpaceNeverChangeDayByDb = cleanSpaceNeverChangeDayByDb.trim();
            String[] sparr = cleanSpaceNeverChangeDayByDb.split(";");
            for (String sp : sparr) {
                sp = sp.trim();
                String[] kv = sp.split(":");
                if (kv.length != 2) {
                    continue;
                }
                float v = CommonUtil.parseFloat(kv[1], 0);
                long vl = (long) (v * 24 * 3600 * 1000);
                //防止float精度问题
                if (StringUtils.isBlank(kv[0]) || vl < 1000) {
                    continue;
                }
                cleanSpaceNeverChangeMillisByDbMap.put(kv[0], vl);
            }
        }
        //Table级别的清理策略
        if (!CommonUtil.isEmpty(cleanSpaceNeverChangeDayByTable)) {
            cleanSpaceNeverChangeDayByTable = cleanSpaceNeverChangeDayByTable.trim();
            String[] sparr = cleanSpaceNeverChangeDayByTable.split(";");
            for (String sp : sparr) {
                sp = sp.trim();
                String[] kv = sp.split(":");
                if (kv.length != 2) {
                    continue;
                }
                float v = CommonUtil.parseFloat(kv[1], 0);
                long vl = (long) (v * 24 * 3600 * 1000);
                //防止float精度问题
                if (StringUtils.isBlank(kv[0]) || vl < 10000) {
                    continue;
                }
                cleanSpaceNeverChangeMillisByTableMap.put(kv[0], vl);
            }

        }
    }

    @Override
    public void run() {
        while (true) {
            logger.info("start to clean data");
            try {
                File baseFile = new File(this.baseDir);
                for (File dataBase : baseFile.listFiles()) {    //数据库dataBase
                    if (dataBase.isFile()) {    //文件跳过
                        continue;
                    }
                    //先走默认，再判断是否走库级别的
                    long cleanSpaceNeverChangeMillisByDb = this.cleanSpaceNeverChangeMillis;
                    //如果有DB失效配置，走DB的失效配置
                    Long dbl = cleanSpaceNeverChangeMillisByDbMap.get(dataBase.getName());
                    if (dbl != null) {
                        cleanSpaceNeverChangeMillisByDb = dbl;
                    }
                    for (File table : dataBase.listFiles()) {   //表数据
                        if (table.isFile()) {   //如果是文件肯定不是表,跳过
                            continue;
                        }
                        if (CLEAN_EXCLUDE_TABLES.contains(table.getName())) {
                            continue;
                        }
                        //表级的清理策略默认跟DB的策略走
                        long cleanSpaceNeverChangeMillisByTable = cleanSpaceNeverChangeMillisByDb;
                        Long tbl = cleanSpaceNeverChangeMillisByTableMap.get(table.getName());
                        //如果有表级别的配置才跟表走配置
                        if (tbl != null) {
                            cleanSpaceNeverChangeMillisByTable = tbl;
                        }
                        File seqFile = new File(table.getPath() + "/" + Constants.TABLE_SEQUENCE);
                        //当sequence.txt不存在时候  seqFile.lastModified() = 0, 也会出发删除表操作
                        if (this.cleanTableNeverChangeMillis > 0 && seqFile.lastModified() > 0 && System.currentTimeMillis() - seqFile.lastModified() > this.cleanTableNeverChangeMillis) {    //删除表
                            deleteDir(table);
                            continue;  //表都删了, 后面就不执行了
                        }
                        File rows = new File(table.getPath() + "/" + Constants.ROWS_NAME);
                        for (File space : rows.listFiles()) {
                            FileFilter fileFilter = new FileFilter() {
                                @Override
                                public boolean accept(File pathname) {
                                    if (pathname.isFile() && pathname.getName().endsWith(Constants.SPACE_SUFFIX)) {
                                        return true;
                                    }
                                    return false;
                                }
                            };
                            File[] spaceArray = space.listFiles(fileFilter);
                            if (spaceArray.length == 0) {   //没有找到索引文件, 说明当前分区应该是损坏的,直接删除
                                //deleteDir(space); //先不删除，防止索引不存在，误删
                            } else {
                                for (File spaceIdxFile : spaceArray) {
                                    if (cleanSpaceNeverChangeMillisByTable > 0 && spaceIdxFile.lastModified() > 0 && System.currentTimeMillis() - spaceIdxFile.lastModified() > cleanSpaceNeverChangeMillisByTable) {   //删除分区
                                        String spaceName = space.getName();
                                        deleteDir(space);
                                        Integer sp = CommonUtil.parseInt2(spaceName);
                                        if (sp != null) {
                                            List<String> dirs = new ArrayList<>();
                                            for (int i = sp * Constants.SPACE_SIZ, k = sp * Constants.SPACE_SIZ + Constants.SPACE_SIZ; i < k; i++) {
                                                DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
                                                dirs.add(df.format(i));
                                            }
                                            DataCache.clearByClean(dataBase.getName(), table.getName(), dirs);
                                        }

                                    }
                                }
                            }

                        }
                    }
                }
                Thread.sleep(30000);   //休息30秒
            } catch (Exception e) {
                logger.error("clean data fail", e);
            }
        }

    }

    private static void deleteDir(File dir) {
        try {
            logger.info("delete dir " + dir.getPath());
            FileUtils.deleteDirectory(dir);
        } catch (Exception e) {
            logger.error("delete dir fail, dir:" + dir.getPath(), e);
        }
    }

    public static void start() {
        if (Params.getCleanTableNeverChangeDay() == 0L && Params.getCleanSpaceNeverChangeDay() == 0L) {
            logger.info("cleanTableNeverChangeDay and cleanSpaceNeverChangeDay are 0, so don't start clean!");
            return;
        }

        Runnable run = new CleanRunnable(Params.getBaseDir(), Params.getCleanTableNeverChangeDay(), Params.getCleanSpaceNeverChangeDay(), Params.getCleanSpaceNeverChangeDayByDb(), Params.getCleanSpaceNeverChangeDayByTable());
        PoolUtil.CACHED_POLL.execute(run);
    }
}
