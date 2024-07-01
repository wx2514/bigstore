package com.wuqing.business.bigstore.config;

/**
 * Created by wuqing on 17/8/30.
 */
public class ServerConstants {

    public static String DIR_FORMAT = "0000000000";

    public static int DIR_LENGTH = DIR_FORMAT.length();

    /**
     * 是否使用64进制存储数据
     */
    public final static boolean USE_64 = Params.isUse64();

    /**
     * 每个数据块多少行数据
     */
    //public static final long PARK_SIZ = 10000;
    public static final long PARK_SIZ = Params.getParkSize();

    /**
     * 数值分段索引被分为多少个段
     */
    public static final int PARK_INDEX_SIZE = Params.getIndexSize();

    /**
     * 数值分区索引被分为多少段
     */
    //public static final int SPACE_INDEX_SIZE = PARK_INDEX_SIZE * 10;

    /**
     * lineNum长度
     */
    public final static int NUM_LENGTH = String.valueOf(PARK_SIZ).length() - 1;

    /**
     * 特殊符号，标记合并后的文件属于哪一个目录，开始符号
     */
    public final static char DIR_FLAG_START = 3;

    /**
     * 特殊符号，标记合并后的文件属于哪一个目录，结束符号
     */
    public final static char DIR_FLAG_END = 4;

    /**
     * 每行分隔符
     */
    public final static char LINE_SEPARATOR = '\n';

    /**
     * 枚举索引文件，行号分割符
     */
    public static final String DESC_LINE_SPLIT = ",";

    /**
     * 关键字搜索，分为几个索引文件
     */
    public static final int GROUP_SIZE = 10;

    /**
     * 索引 key value 分隔符
     */
    public static final String INDEX_FLAG = "-";

}
