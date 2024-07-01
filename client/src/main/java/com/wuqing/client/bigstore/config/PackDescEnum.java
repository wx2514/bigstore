package com.wuqing.client.bigstore.config;

/**
 * Created by wuqing on 17/3/14.
 * 块字段描述文件定义
 */
public enum PackDescEnum {
    /**
     * 压缩标记
     */
    PRESS,

    /**
     * 总条数
     */
    COUNT,

    /**
     * Null条数,[SUM] 可能只有Null条数，也可能后面追加SUM, 逗号","分割
     */
    NULL_COUNT_OTHERS,

    /**
     * 最小值
     */
    START,

    /**
     * 最大值
     */
    END,
}
