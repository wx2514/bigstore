package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.util.CommonUtil;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wuqing on 17/3/9.
 */
public class ColumnDef implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数值型
     */
    public static final String LONG_TYPE = "l";

    /**
     * 数值型检索
     */
    public static final String LONG_FIND = "f";

    /**
     * 数值型, 且入库基本有序
     */
    public static final String LONG_ORDER_TYPE = "o";

    /**
     * 数字查询
     */
    public static final String DECIMA_TYPE = "d";

    /**
     * 字符型
     */
    public static final String STRING_TYPE = "s";

    /**
     * 字符型检索
     */
    public static final String STRING_FIND = "g";

    /**
     * 字符型分区字段
     * 会建立 布隆 索引
     */
    public static final String STRING_ORDER = "p";

    /**
     * 文本型搜索，建立text字段的倒排索引，可用于全文检索
     */
    public static final String TEXT_INDEX = "r";

    /**
     * 枚举类型
     */
    public static final String STRING_MENU = "m";

    /**
     * 类型和列名分割符
     */
    public static final String SPLIT = "\t";

    /**
     * 集合方式count
     */
    public static final int AGGREGATION_COUNT = 1;

    /**
     * 集合方式sum
     */
    public static final int AGGREGATION_SUM = 2;

    /**
     * 集合方式avg
     */
    public static final int AGGREGATION_AVG = 3;

    /**
     * 集合方式min
     */
    public static final int AGGREGATION_MIN = 4;

    /**
     * 集合方式max
     */
    public static final int AGGREGATION_MAX = 5;

    /**
     * 集合方式count
     */
    public static final int AGGREGATION_COUNT_DISTINCT = 7;

    /**
     * 集合方式distinct
     */
    public static final int AGGREGATION_DISTINCT = 8;

    /**
     * 集合方式: 按秒count
     */
    public static final int SECOND_COUNT = 11;

    /**
     * 集合方式: 按秒sum
     */
    public static final int SECOND_SUM = 12;

    /**
     * 集合方式: 按秒avg
     * 因为多集群的问题，暂时不支持按秒avg
     */
    public static final int SECOND_AVG = 13;

    /**
     * 集合方式: 按秒min
     */
    public static final int SECOND_MIN = 14;

    /**
     * 集合方式: 按秒max
     */
    public static final int SECOND_MAX = 15;

    /**
     * 集合方式: 按秒delta
     * 计算窗口时间内的首尾差值：结尾数 - 开始值
     */
    public static final int SECOND_DELTA = 16;

    /**
     * 集合方式: 按秒count
     */
    public static final int SECOND_COUNT_DISTINCT = 17;

    /**
     * 列名
     */
    private String name;

    /**
     * 类型
     */
    private String type;

    /**
     * 长度
     */
    private int length;

    /**
     * 小数的长度（位数）
     */
    private int decimalLength;


    /**
     * 查询字段聚合方式，紧在select时，判定是否为集合函数
     */
    private int aggregation;

    /**
     * 剩下的聚合方式（除了最内层的）
     */
    private List<Integer> otherAggregation;

    /**
     * 集合窗口时间（单位：秒）
     */
    private int windowSecond;

    /**
     * 存储的原始行，直接用于拆分
     * @param line
     */
    public ColumnDef(String line) {
        String[] array = line.split(SPLIT);
        if (array[0].length() == 1) {
            this.type = array[0];
        } else {
            this.type = array[0].substring(0, 1);
            this.length = CommonUtil.parseInt(array[0].substring(1));
        }
        this.name = array[1];
        if (array.length == 3) {    //追加小数部分长度
            this.decimalLength = CommonUtil.parseInt(array[2]);
        }
    }

    private void check() {
        if (this.type == ColumnDef.DECIMA_TYPE && this.decimalLength <= 0) {
            throw new IllegalArgumentException("decimalLength is invalid, when type is decima");
        }
        if (this.decimalLength > 10) {
            throw new IllegalArgumentException("decimalLength is too long, decimalLength: " + decimalLength);
        }
    }

    /**
     * 赋值name, type
     * @param name
     * @param type
     */
    public ColumnDef(String name, String type) {
        this.name = name;
        this.type = type;
        check();
    }

    /**
     * 赋值name, type
     * @param name
     * @param type
     */
    public ColumnDef(String name, String type, int length) {
        this.name = name;
        this.type = type;
        this.length = length;
        check();
    }

    /**
     * 创建 ColumnDef
     * @param name
     * @param type
     * @param length
     * @param decimalLength
     */
    public ColumnDef(String name, String type, int length, int decimalLength) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.decimalLength = decimalLength;
        check();
    }

    @Override
    public ColumnDef clone() {
        ColumnDef def = new ColumnDef(this.name, this.type, this.length, this.decimalLength);
        def.setAggregation(this.aggregation);
        def.setWindowSecond(this.windowSecond);

        return def;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getAggregation() {
        return aggregation;
    }

    public void setAggregation(int aggregation) {
        this.aggregation = aggregation;
    }

    public List<Integer> getOtherAggregation() {
        return otherAggregation;
    }

    public void setOtherAggregation(List<Integer> otherAggregation) {
        this.otherAggregation = otherAggregation;
    }

    public int getWindowSecond() {
        return windowSecond;
    }

    public void setWindowSecond(int windowSecond) {
        this.windowSecond = windowSecond;
    }

    public int getDecimalLength() {
        return decimalLength;
    }

    public void setDecimalLength(int decimalLength) {
        this.decimalLength = decimalLength;
    }

    /**
     * 是否是数值类型
     * @return
     */
    public boolean isLong() {
        return LONG_TYPE.equals(this.type) || LONG_FIND.equals(this.type) || LONG_ORDER_TYPE.equals(this.type);
    }

    public boolean isDecimal() {
        return DECIMA_TYPE.equals(this.type);
    }

    public boolean isString() {
        return STRING_TYPE.equals(this.type)
                || STRING_FIND.equals(this.type)
                || STRING_ORDER.equals(this.type)
                || TEXT_INDEX.equals(this.type);

    }

    /**
     * 是否是顺序列数值
     * @return
     */
    public boolean isOrder() {
        return LONG_ORDER_TYPE.equals(this.type);
    }

    /**
     * 是否是顺序列字符串
     * @return
     */
    public boolean isOrderString() {
        return STRING_ORDER.equals(this.type);
    }

    /**
     * 是否是表分区字段
     * @return
     */
    public boolean isOrderSpace() {
        return LONG_ORDER_TYPE.equals(this.type) || STRING_ORDER.equals(this.type);
    }

    /**
     * 是否建立倒排索引
     * @return
     */
    public boolean isReverseIndex() {
        return TEXT_INDEX.equals(this.type);
    }


    public int getLength() {
        return length;
    }

    public ColumnDef setLength(int length) {
        this.length = length;
        return this;
    }

    public boolean isEnum() {
        return STRING_MENU.equals(this.type);
    }

    /**
     * 判定是否是查询字段
     * 数字类型建立直方图索引
     * 字符串类型建立位图索引
     * @return
     */
    public boolean isFindCol() {
        return LONG_ORDER_TYPE.equals(this.type)
                || LONG_FIND.equals(this.type)
                || STRING_FIND.equals(this.type)
                || STRING_MENU.equals(this.type);
    }

    public String getDesc() {
        String desc = null;
        if (LONG_TYPE.equals(this.type)) {
            desc = "数值类型";
        } else if (LONG_ORDER_TYPE.equals(this.type)) {
            desc = "数值类型(分区字段)";
        } else if (STRING_TYPE.equals(this.type)) {
            desc = "字符类型";
        } else if (LONG_FIND.equals(this.type)) {
            desc = "数值类型(分段索引)";
        } else if (STRING_FIND.equals(this.type)) {
            desc = "字符类型(字符位索引)";
        } else if (TEXT_INDEX.equals(this.type)) {
            desc = "字符类型(分词索引)";
        } else if (STRING_MENU.equals(this.type)) {
            desc = "枚举类型";
        } else if (DECIMA_TYPE.equals(this.type)) {
            desc = "小数类型";
        } else {
            desc = "未知类型";
        }
        return desc;
    }

    public static boolean isCount(int aggregation) {
        return ColumnDef.SECOND_COUNT == aggregation || ColumnDef.AGGREGATION_COUNT == aggregation;
    }

    public static boolean isDistinct(int aggregation) {
        return ColumnDef.AGGREGATION_DISTINCT == aggregation;
    }

    public static boolean isCountDistinct(int aggregation) {
        return ColumnDef.SECOND_COUNT_DISTINCT == aggregation || ColumnDef.AGGREGATION_COUNT_DISTINCT == aggregation;
    }

    public static boolean isAvg(int aggregation) {
        return ColumnDef.SECOND_AVG == aggregation || ColumnDef.AGGREGATION_AVG == aggregation;
    }

    public static boolean isSum(int aggregation) {
        return ColumnDef.SECOND_SUM == aggregation || ColumnDef.AGGREGATION_SUM == aggregation;
    }

    public static boolean isMin(int aggregation) {
        return ColumnDef.SECOND_MIN == aggregation || ColumnDef.AGGREGATION_MIN == aggregation;
    }

    public static boolean isMax(int aggregation) {
        return ColumnDef.SECOND_MAX == aggregation || ColumnDef.AGGREGATION_MAX == aggregation;
    }

    @Override
    public String toString() {
        String desc = type + (this.length == 0 ? "" : String.valueOf(length)) + SPLIT + name;
        if (decimalLength > 0) {
            desc = desc + SPLIT + decimalLength;
        }
        return desc;
    }
}
