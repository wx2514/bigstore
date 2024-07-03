package com.wuqing.client.bigstore.config;

/**
 * Created by wuqing on 17/2/17.
 */
public class FileConfig {

    /**
     * 数据文件后缀
     */
    public static final String DATA_FILE_SUFFIX_PRE = "_data";

    /**
     * 索引文件后缀
     */
    public static final String DATA_INDEX_SUBFFIX = ".txt";

    /**
     * 数据文件后缀
     */
    public static final String DATA_FILE_SUFFIX = DATA_FILE_SUFFIX_PRE + ".txt";

    /**
     * 索引号
     */
    public static final String SPLIT_WORD_IDX = "{idx}";

    /**
     * 分词文件后缀
     */
    public static final String SPLIT_WORD_SUFFIX = "_word" + SPLIT_WORD_IDX + ".txt";

    /**
     * 压缩数据文件后缀
     */
    public static final String DATA_FILE_GZ_SUFFIX = "_data.gz";

    /**
     * 压缩数据文件后缀
     */
    public static final String DATA_FILE_BZ2_SUFFIX = "_data.bz";

    /**
     * 压缩数据文件后缀
     */
    public static final String DATA_FILE_XZ_SUFFIX = "_data.xz";

    /**
     * 描述文件后缀
     */
    public static final String DESC_FILE_SUFFIX = "_desc.txt";

    /**
     * 布隆表达式文件路径前缀
     */
    public static final String BOLLM_TYPE_PATH = "BLOOM_PATH:";

    /**
     * 布隆表达式文件路径前缀 的 字符串长度
     */
    public static final int BOLLM_TYPE_LENGTH = BOLLM_TYPE_PATH.length();

    /**
     * 描述文件后缀
     */
    public static final String BOLLM_FILE_SUFFIX = "_bloom.bt";

    /**
     * 表分区索引文件
     */
    public static final String SPACE_FILE_SUFFIX = "_space.txt";

    /**
     * 表分区索引文件
     */
    public static final String SPACE_BLOOM_SUFFIX = "_space.bt";

    /**
     * 倒排索引目录
     */
    public static final String INDEX_FILE_SUFFIX = "_index";

    /**
     * 索引分隔符
     * 1639389320438#1639389320439-1
     */
    public static final String INDEX_SPILT = "#";

    /**
     * 索引分隔符
     * 1639389320438#1639389320439-1
     */
    public static String INDEX_FLAG = "-";

    /**
     * nullCount SUM 分割符
     * 0,12345678
     */
    public static String NULL_SUM_SPLIT = ",";

    /**
     * 已经同步标记
     */
    public static String SEND_FLAG = "_send.flg";

    /**
     * 已经同步标记
     */
    public static String COMPRESS_FLAG = "_compress.flt";

    /**
     * 已经同步标记
     */
    public static String CACHE_FLAG = "_cache.flg"; //主从同步时，不要把这个文件同步过去

    /**
     * 已经合并标识
     */
    public static String MERGER_FLAG = "_merger.flg";


}
