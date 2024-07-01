package com.wuqing.client.bigstore.config;

import com.wuqing.client.bigstore.bean.SpecialFieldEnum;
import org.apache.commons.io.Charsets;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wuqing on 17/3/9.
 */
public class Constants {

    /**
     * 默认数据库
     */
    public static String DEFAULT_DATA_BASE = "default_data_base";

    /**
     * 每个块的索引分成多个区间
     */
    //public static final int PARK_INDEX_SIZE = 128;

    /**
     * 根据字符串前_个字节，建立位图索引
     */
    public final static int STRING_INDEX_COUNT = 32;

    /**
     * 每个表表分区多少快数据
     */
    public static final int SPACE_SIZ = 100;

    /**
     * 索引范围存在的标志位
     */
    public static final int INDEX_EXSIT_INT = 1;

    /**
     * 索引范围存在的标志位
     */
    public static final String INDEX_EXSIT = String.valueOf(INDEX_EXSIT_INT);

    /**
     * 索引范围不存在的标志位
     */
    public static final String INDEX_NOT_EXSIT = "0";

    /**
     * 行号与正文分隔符
     */
    //public final static char STACK_SPLIT = '\t';
    public final static char STACK_SPLIT = 2;   //特殊符号，标记正文开始

    /**
     * 用于替换换行符
     */
    public final static char LINE_BREAK_REPLACE = 3;   //特殊符号，标记正文开始
    //public static String LINE_BREAK_REPLACE = "#{$@$}";

    /**
     * 每行分隔符
     */
    public final static char LINE_SEPARATOR = '\n';

    /**
     * 压缩标识：未压缩
     */
    public final static String UN_PRESS = "0";

    /**
     * 压缩标识：已压缩
     */
    //public final static String PRESS = "1";

    /**
     * 默认字符集
     */
    public final static Charset DEFAULT_CHARSET = Charsets.UTF_8;

    /**
     * 序列ID，未使用的
     */
    public final static String TABLE_SEQUENCE = "sequence.txt";

    /**
     * 不相关的
     */
    public final static int RELATION_TAG_NONE = -1;

    /**
     * 可能部分数据相关
     */
    public final static int RELATION_TAG_MAYBE = 0;

    /**
     * 强相关，所有数据都符合
     */
    public final static int RELATION_TAG_ALL = 1;

    /**
     * 查询类型 相等
     */
    public final static int QUERY_TYPE_EQUAL = 0;

    /**
     * 查询类型 范围
     */
    public final static int QUERY_TYPE_RANGE = 1;

    /**
     * 查询类型 模糊匹配
     */
    public final static int QUERY_TYPE_LIKE = 2;

    /**
     * 全文检索 范围
     */
    public final static int QUERY_TYPE_FULLTEXT_RETRIEVAL = 3;

    /**
     * grep 模式 模糊匹配
     */
    public final static int QUERY_TYPE_GREP = 4;

    /**
     * 单字段 多值查询 (对应 searchList)
     * 关系 and
     */
    public final static int QUERY_TYPE_KEY_LIKE_AND = 5;

    /**
     * 单字段 多值查询 (对应 searchList)
     * 关系 or
     */
    public final static int QUERY_TYPE_KEY_LIKE_OR = 6;

    /**
     * 查询类型 不等于
     * 备注：只能用于非null字段 检索
     */
    public final static int QUERY_TYPE_NOT_EQUAL = 7;

    /**
     * 查询类型 not模糊匹配
     * 备注：只能用于非null字段 检索
     */
    public final static int QUERY_TYPE_NOT_LIKE = 8;

    /**
     * 查询类型 in
     * 备注：只能用于非null字段 检索
     */
    public final static int QUERY_TYPE_IN = 9;

    /**
     * ID列名
     */
    public final static String COLUMN_ID = "id";

    /**
     * 列枚举定义
     */
    public final static String COL_ENUM_TXT = "_enum.txt";

    /**
     * 网络传输分割符
     */
    public final static byte[] SPLIT = new byte[]{'-', '#', '$', '+', '\r', '\n'};

    /**
     * 访问端口
     */
    public final static int PORT = 60000;

    /**
     * 每次请求超过这个数值的数据 会被丢弃
     */
    public final static int MAX_FRAME = 1024 * 1024 * 1024; //单次最大1G 数据

    /**
     * 失败文件的后缀
     */
    public final static String FILE_SUFFIX = "-fail";

    /**
     * IP和端口的分隔符
     */
    public final static String IP_PORT_SPLIT = ":";

    /**
     * 集群IP分隔符
     */
    public final static String IP_GROUP_SPLIT = ",";

    /**
     * 行目录名
     */
    public final static String ROWS_NAME = "rows";

    /**
     * 客户端提交时候的列分割符号
     * 等级1 (数值越大优先级越高)
     */
    public final static String COLUMN_SPLIT_LEVEL_1 = "|";

    /**
     * 客户端提交时候的列分割符号原始char [标题开始] 符
     */
    public final static char COLUMN_SPLIT_2_CHAR = 1;

    /**
     * 客户端提交时候的列分割符号
     * 等级1 (数值越大优先级越高)
     */
    public final static String COLUMN_SPLIT_2 = COLUMN_SPLIT_2_CHAR + "";

    /**
     * 特殊的 feild
     */
    public final static List<String> SPECIAL_FEILD = Arrays.asList(SpecialFieldEnum.ID.name(), SpecialFieldEnum.WINDOW_START.name());

    /**
     * 分区索引后缀
     */
    public final static String SPACE_SUFFIX = "_space.txt";

    public static class SqlOperator {

        public static final String IN = "IN";

        public static final String NOT_EQUAL = "!=";

        public static final String EQUAL = "=";

        public static final String SELECT = "SELECT";

        public static final String FROM = "FROM";

        public static final String WHERE = "WHERE";

        public static final String AND = "AND";

        public static final String BETWEEN = "BETWEEN";

        public static final String SEARCH = "SEARCH";

        public static final String GREP = "GREP";

        public static final String LIKE = "LIKE";

        public static final String NOT_LIKE = "NOT LIKE";

        public static final String LIKE_OR = "LIKEOR";

        public static final String LIKE_AND = "LIKEAND";

        public static final String LIMIT = "LIMIT";

        public static final String GROUP = "GROUP";

        public static final String BY = "BY";

        public static final String AS = "AS";

        public static final String NOT = "NOT";

    }

    public static class Commond {
        public static final String HEART_BEAT_CMD = "HeartBeat";
    }


}
