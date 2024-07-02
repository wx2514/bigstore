package com.wuqing.business.bigstore.config;

import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by wuqing on 17/4/4.
 */
public class Params {

    private static final Logger logger = LoggerFactory.getLogger(Params.class);

    public static String PARAMS_PROPERTIES_PATH = "/params.properties";

    private static final Params PARAMS = new Params();

    private String baseDir;

    private int port = Constants.PORT;

    private int dataCacheSize;

    private int queryCacheSize;

    private boolean aggregationCache = true;

    private int indexCacheSize;

    private int indexEnumCacheSize;

    private int tableCacheSize;

    private int spaceCacheSize;

    private int enumCacheSize;

    private int queryThreadSize;

    private int writeThreadSize;

    private int nettyThreadSize = 10;

    private int compressThreadSize;

    private int hashcodeRetainSize = 3; //分词索引中hashCode保留几位

    private String groupIps;

    private String[] groupIpArray = new String[0];

    private String localIp;

    private boolean slave = false;  //默认是master

    private String slaveIp;

    private int slavePort = Constants.PORT;  //默认是master

    private boolean syncCompress = false;   //主从同步文件后，是否进行文件压缩，默认false

    private boolean loadIndex = false;  //默认是false，不预加载索引

    private final List<String> loadFields = new ArrayList<String>();

    private int sendWaitHour = 1;   //默认1小时

    private int scanTableHour = 108;   //默认108小时

    private int scanDpHour = 108;   //默认108小时

    private float compressDelayHour = 72;    //默认72小时

    private int forceSendMinute = 20; //默认20分钟

    private int parkSize = 100000;  //默认10万

    private int indexSize = 1024;  //默认1024

    private boolean mergerData = false;

    private boolean use64 = true;

    private List<String> cleanExcludeTables = new ArrayList<>();

    /**
     * 压缩类型
     * 0: gzip压缩
     * 1: bzip2压缩
     * 2: xz压缩
     */
    private int compressType;

    private float cleanTableNeverChangeDay = 0; //默认0不清理

    private float cleanSpaceNeverChangeDay = 0; //默认0不清理

    private String cleanSpaceNeverChangeDayByDb; //默认0不清理

    private String cleanSpaceNeverChangeDayByTable; //默认0不清理

    private int forceFlushStoreSecond = 30;    //默认30秒强制flush store

    private int forceFlushLineCount = 100000;   //默认10万条强制flush store

    private Set<String> queryCacheColumnsSet = new HashSet<>();

    private Params() {
        InputStream inputStream = null;
        try {
            String filePath = "etc" + PARAMS_PROPERTIES_PATH;
            File file = new File(filePath);
            if (file.exists()) {
                logger.info("load properties from " + filePath);
                inputStream = new FileInputStream(file);
            }  else {
                logger.info("load properties from " + filePath + " in classpath");
                inputStream = Params.class.getResourceAsStream(PARAMS_PROPERTIES_PATH);
            }
            Properties p = new Properties();
            p.load(inputStream);
            baseDir = p.getProperty("baseDir");
            String portStr = p.getProperty("port");
            if (portStr != null && portStr.length() > 0) {
                port = Integer.parseInt(portStr);
            }
            dataCacheSize = Integer.parseInt(p.getProperty("dataCacheSize"));
            queryCacheSize = Integer.parseInt(p.getProperty("queryCacheSize"));
            indexCacheSize = Integer.parseInt(p.getProperty("indexCacheSize"));
            indexEnumCacheSize = Integer.parseInt(p.getProperty("indexEnumCacheSize"));
            tableCacheSize = Integer.parseInt(p.getProperty("tableCacheSize"));
            spaceCacheSize = Integer.parseInt(p.getProperty("spaceCacheSize"));
            enumCacheSize = Integer.parseInt(p.getProperty("enumCacheSize"));
            queryThreadSize = Integer.parseInt(p.getProperty("queryThreadSize"));
            writeThreadSize = Integer.parseInt(p.getProperty("writeThreadSize"));
            nettyThreadSize = CommonUtil.parseInt(p.getProperty("nettyThreadSize"), nettyThreadSize);
            hashcodeRetainSize = CommonUtil.parseInt(p.getProperty("hashcodeRetainSize"), hashcodeRetainSize);
            compressType = Integer.parseInt(p.getProperty("compressType"));
            compressThreadSize = Integer.parseInt(p.getProperty("compressThreadSize"));
            groupIps = p.getProperty("groupIps");
            if (!CommonUtil.isEmpty(groupIps)) {
                groupIpArray = groupIps.split(Constants.IP_GROUP_SPLIT);
            }
            localIp = InetAddress.getLocalHost().getHostAddress();
            String slv = p.getProperty("slave");
            if ("true".equals(slv)) {
                slave = true;
            } else if (!CommonUtil.isEmpty(slv) && !"false".equals(slv)) {
                slv = slv.trim();
                String[] ipPort =  slv.split(":");
                slaveIp = ipPort[0];
                if (ipPort.length == 2) {
                    slavePort = Integer.parseInt(ipPort[1]);
                }
            }
            String syncCompressStr = p.getProperty("syncCompress");
            if ("true".equals(syncCompressStr)) {
                syncCompress = true;
            }
            String loadIndexStr = p.getProperty("loadIndex");
            if ("true".equals(loadIndexStr)) {
                loadIndex = true;
            }
            String use64Str = p.getProperty("use64");
            if ("false".equals(use64Str)) {
                use64 = false;
            }
            String aggregationCacheStr = p.getProperty("aggregationCache");
            if ("false".equals(aggregationCacheStr)) {  //默认是true, 只有强制设置成false的时候才赋值false
                aggregationCache = false;
            }
            String loadFieldsStr = p.getProperty("loadFields");
            if (!CommonUtil.isEmpty(loadFieldsStr)) {
                for (String s : loadFieldsStr.split(",")) {
                    loadFields.add(s);
                }
            }
            sendWaitHour = CommonUtil.parseInt(p.getProperty("sendWaitHour"), sendWaitHour);
            scanTableHour = CommonUtil.parseInt(p.getProperty("scanTableHour"), scanTableHour);
            scanDpHour = CommonUtil.parseInt(p.getProperty("scanDpHour"), scanDpHour);
            compressDelayHour = CommonUtil.parseFloat(p.getProperty("compressDelayHour"), compressDelayHour);
            forceSendMinute = CommonUtil.parseInt(p.getProperty("forceSendMinute"), forceSendMinute);
            parkSize = CommonUtil.parseInt(p.getProperty("parkSize"), parkSize);
            indexSize = CommonUtil.parseInt(p.getProperty("indexSize"), indexSize);
            String mergerDataStr = p.getProperty("mergerData");
            if ("true".equals(mergerDataStr)) {
                mergerData = true;
            }
            cleanTableNeverChangeDay = CommonUtil.parseFloat(p.getProperty("cleanTableNeverChangeDay"), cleanTableNeverChangeDay);
            cleanSpaceNeverChangeDay = CommonUtil.parseFloat(p.getProperty("cleanSpaceNeverChangeDay"), cleanSpaceNeverChangeDay);;
            cleanSpaceNeverChangeDayByDb = p.getProperty("cleanSpaceNeverChangeDayByDb");
            cleanSpaceNeverChangeDayByTable = p.getProperty("cleanSpaceNeverChangeDayByTable");
            forceFlushStoreSecond = CommonUtil.parseInt(p.getProperty("forceFlushStoreSecond"), forceFlushStoreSecond);
            forceFlushLineCount = CommonUtil.parseInt(p.getProperty("forceFlushLineCount"), forceFlushLineCount);
            String queryCacheColumns = p.getProperty("queryCacheColumns");
            if (StringUtils.isNotBlank(queryCacheColumns)) {
                String[] cols = queryCacheColumns.split(",");
                for (String col : cols) {
                    queryCacheColumnsSet.add(col.trim());
                }
            }

            String cleanExcludeTablesStr = p.getProperty("cleanExcludeTables");
            if (!CommonUtil.isEmpty(cleanExcludeTablesStr)) {
                cleanExcludeTablesStr = cleanExcludeTablesStr.trim();
                String[] arry = cleanExcludeTablesStr.split(",");
                for (String s : arry) {
                    s = s.trim();
                    if (!CommonUtil.isEmpty(s)) {
                        cleanExcludeTables.add(s);
                    }
                }
            }

            logger.info("init params:\nbaseDir:" + baseDir + "\nlocalIp:" + localIp + "\nport:" + port  + "\ndataCacheSize:" + dataCacheSize + "\nqueryCacheSize:" + queryCacheSize
                    + "\nindexCacheSize:" + indexCacheSize + "\nindexEnumCacheSize:" + indexEnumCacheSize + "\ntableCacheSize:" + tableCacheSize + "\nspaceCacheSize:" + spaceCacheSize
                    + "\nenumCacheSize:" + enumCacheSize + "\naggregationCache:" + aggregationCache + "\nqueryThreadSize:" + queryThreadSize + "\nwriteThreadSize:" + writeThreadSize + "\nnettyThreadSize:" + nettyThreadSize
                    + "\ncompressType:" + compressType + "\ncompressThreadSize:" + compressThreadSize + "\ngroupIps:" + groupIps + "\nslave:" + slave + "\nslaveIp:" + slaveIp + "\nsyncCompress:" + syncCompress
                    + "\nloadIndex:" + loadIndex  + "\nloadFields:" + loadFields + "\nsendWaitHour:" + sendWaitHour + "\nscanTableHour:" + scanTableHour
                    + "\nscanDpHour:" + scanDpHour + "\ncompressDelayHour:" + compressDelayHour + "\nforceSendMinute:" + forceSendMinute
                    + "\nparkSize:" + parkSize + "\nindexSize:" + indexSize + "\nmergerData:" + mergerData
                    + "\ncleanTableNeverChangeDay:" + cleanTableNeverChangeDay + "\ncleanSpaceNeverChangeDay:" + cleanSpaceNeverChangeDay + "\ncleanSpaceNeverChangeDayByDb:" + cleanSpaceNeverChangeDayByDb + "\ncleanSpaceNeverChangeDayByTable:" + cleanSpaceNeverChangeDayByTable
                    + "\nforceFlushStoreSecond:" + forceFlushStoreSecond + "\nforceFlushLineCount:" + forceFlushLineCount
                    + "\nqueryCacheColumns:" + queryCacheColumns + "\nuse64:" + use64 + "\ncleanExcludeTables:" + cleanExcludeTablesStr);

        } catch (Exception e) {
            logger.error("load params.properties fail.", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    public static int getDataCacheSize() {
        return PARAMS.dataCacheSize;
    }

    public static int getQueryCacheSize() {
        return PARAMS.queryCacheSize;
    }

    public static int getIndexCacheSize() {
        return PARAMS.indexCacheSize;
    }

    public static int getTableCacheSize() {
        return PARAMS.tableCacheSize;
    }

    public static int getSpaceCacheSize() {
        return PARAMS.spaceCacheSize;
    }

    public static int getEnumCacheSize() {
        return PARAMS.enumCacheSize;
    }

    public static int getQueryThreadSize() {
        return PARAMS.queryThreadSize;
    }

    public static int getWriteThreadSize() {
        return PARAMS.writeThreadSize;
    }

    public static int getCompressThreadSize() {
        return PARAMS.compressThreadSize;
    }

    public static int getCompressType() {
        return PARAMS.compressType;
    }

    public static String getBaseDir() {
        return PARAMS.baseDir;
    }

    public static int getPort() {
        return PARAMS.port;
    }

    public static String getGroupIps() {
        return PARAMS.groupIps;
    }

    public static String[] getGroupIpArray() {
        return PARAMS.groupIpArray;
    }

    public static String getLocalIp() {
        return PARAMS.localIp;
    }

    public static boolean isSlave() {
        return PARAMS.slave;
    }

    public static String getSlaveIp() {
        return PARAMS.slaveIp;
    }

    public static int getSlavePort() {
        return PARAMS.slavePort;
    }

    public static boolean isLoadIndex() {
        return PARAMS.loadIndex;
    }

    public static int getSendWaitHour() {
        return PARAMS.sendWaitHour;
    }

    public static int getScanTableHour() {
        return PARAMS.scanTableHour;
    }

    public static int getScanDpHour() {
        return PARAMS.scanDpHour;
    }

    public static float getCompressDelayHour() {
        return PARAMS.compressDelayHour;
    }

    public static int getForceSendMinute() {
        return PARAMS.forceSendMinute;
    }

    public static int getParkSize() {
        return PARAMS.parkSize;
    }

    public static boolean isMergerData() {
        return PARAMS.mergerData;
    }

    public static int getIndexEnumCacheSize() {
        return PARAMS.indexEnumCacheSize;
    }

    public static List<String> getLoadFields() {
        return PARAMS.loadFields;
    }

    public static boolean isSyncCompress() {
        return PARAMS.syncCompress;
    }

    public static int getIndexSize() {
        return PARAMS.indexSize;
    }

    public static int getNettyThreadSize() {
        return PARAMS.nettyThreadSize;
    }

    public static int getHashcodeRetainSize() {
        return PARAMS.hashcodeRetainSize;
    }

    public static float getCleanTableNeverChangeDay() {
        return PARAMS.cleanTableNeverChangeDay;
    }

    public static float getCleanSpaceNeverChangeDay() {
        return PARAMS.cleanSpaceNeverChangeDay;
    }

    public static int getForceFlushStoreSecond() {
        return PARAMS.forceFlushStoreSecond;
    }

    public static int getForceFlushLineCount() {
        return PARAMS.forceFlushLineCount;
    }

    public static Set<String> getQueryCacheColumnsSet() {
        return PARAMS.queryCacheColumnsSet;
    }

    public static boolean isUse64() {
        return PARAMS.use64;
    }

    public static boolean isAggregationCache() {
        return PARAMS.aggregationCache;
    }

    public static List<String> getCleanExcludeTables() {
        return PARAMS.cleanExcludeTables;
    }

    public static String getCleanSpaceNeverChangeDayByDb() {
        return PARAMS.cleanSpaceNeverChangeDayByDb;
    }

    public static String getCleanSpaceNeverChangeDayByTable() {
        return PARAMS.cleanSpaceNeverChangeDayByTable;
    }
}

