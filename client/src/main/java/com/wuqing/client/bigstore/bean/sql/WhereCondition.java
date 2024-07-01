package com.wuqing.client.bigstore.bean.sql;

import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.exception.SqlException;

import java.util.List;

/**
 * Created by wuqing on 18/5/31.
 */
public class WhereCondition {

    /**
     * 查询：不等于
     */
    public static int QUERY_TYPE_NOT_EQUAL = Constants.QUERY_TYPE_NOT_EQUAL;

    /**
     * 查询：等于
     */
    public static int QUERY_TYPE_EQUAL = Constants.QUERY_TYPE_EQUAL;

    /**
     * 查询：分词检索
     */
    public static int QUERY_TYPE_SEARCH = Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL;

    /**
     * 查询：字符匹配（类似grep）
     */
    public static int QUERY_TYPE_GREP = Constants.QUERY_TYPE_GREP;

    /**
     * 不like
     */
    public static int QUERY_TYPE_NOT_LIKE = Constants.QUERY_TYPE_NOT_LIKE;

    /**
     * 查询：模糊查询
     */
    public static int QUERY_TYPE_LIKE = Constants.QUERY_TYPE_LIKE;

    /**
     * 查询：范围查询
     */
    public static int QUERY_TYPE_RANGE = Constants.QUERY_TYPE_RANGE;

    /**
     * 单字段 多值查询 (对应 searchList)
     * 关系 and
     */
    public static int QUERY_TYPE_KEY_LIKE_AND = Constants.QUERY_TYPE_KEY_LIKE_AND;

    /**
     * 单字段 多值查询 (对应 searchList)
     * 关系 or
     */
    public static int QUERY_TYPE_KEY_LIKE_OR = Constants.QUERY_TYPE_KEY_LIKE_OR;

    /**
     * 单字段 多值查询 (对应 searchList)
     * 关系 and
     */
    public static int QUERY_TYPE_IN = Constants.QUERY_TYPE_IN;

    private int type;

    public WhereCondition(String key, String value, int type) {
        if (key.indexOf(" ") > -1 || key.indexOf("\n") > -1
                || key.indexOf("\r") > -1 || key.indexOf("\f") > -1) {
            throw new SqlException("列名无效:" + key);
        }
        this.key = key;
        if (value.startsWith("'") && value.endsWith("'")) {
            this.value = value.substring(1, value.length() - 1);
        } else {
            this.value = Long.parseLong(value);
        }
        this.type = type;
    }

    public WhereCondition(String key, long start, long end) {
        this.key = key;
        QueryRange range = new QueryRange(start, end);
        this.value = range;
        this.type = QUERY_TYPE_RANGE;
    }

    public WhereCondition(String key, List<String> values, int type) {
        this.key = key;
        for (int i = 0, k = values.size(); i < k; i++) {
            String s = values.get(i);
            if (s.startsWith("'") && s.endsWith("'")) {
                s = s.substring(1, s.length() - 1);
                values.set(i, s);
            } else {
                if (type != Constants.QUERY_TYPE_IN) {  //如果不是in的话，只能是字符串
                    throw new RuntimeException("Condition value is invalid, please add '. s:" + s);
                }
            }
        }
        this.value = values;
        this.type = type;
    }

    private String key;

    private Object value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isEqual() {
        return this.type == QUERY_TYPE_EQUAL;
    }

    public boolean isNotEqual() {
        return this.type == QUERY_TYPE_NOT_EQUAL;
    }

    public boolean isSearch() {
        return this.type == QUERY_TYPE_SEARCH;
    }

    public boolean isGrep() {
        return this.type == QUERY_TYPE_GREP;
    }

    public boolean isLike() {
        return this.type == QUERY_TYPE_LIKE;
    }

    public boolean isNotLike() {
        return this.type == QUERY_TYPE_NOT_LIKE;
    }

    public boolean isLikeOr() {
        return this.type == QUERY_TYPE_KEY_LIKE_OR;
    }

    public boolean isLikeAnd() {
        return this.type == QUERY_TYPE_KEY_LIKE_AND;
    }

    public boolean isRange() {
        return this.type == QUERY_TYPE_RANGE;
    }

    public boolean isIn() {
        return this.type == QUERY_TYPE_IN;
    }

    public String getOperate() {
        if (isEqual()) {
            return Constants.SqlOperator.EQUAL;
        } else if (isNotEqual()) {
            return Constants.SqlOperator.NOT_EQUAL;
        } else if (isSearch()) {
            return Constants.SqlOperator.SEARCH;
        } else if (isGrep()) {
            return Constants.SqlOperator.GREP;
        } else if (isLike()) {
            return Constants.SqlOperator.LIKE;
        } else if (isLikeOr()) {
            return Constants.SqlOperator.LIKE_OR;
        } else if (isNotLike()) {
            return Constants.SqlOperator.NOT_LIKE;
        } else if (isLikeAnd()) {
            return Constants.SqlOperator.LIKE_AND;
        } else if (isIn()) {
            return Constants.SqlOperator.IN;
        }
        return null;
    }
}
