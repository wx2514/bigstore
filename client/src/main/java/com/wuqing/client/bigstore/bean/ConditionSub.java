package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.config.Constants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/15.
 */
public class ConditionSub implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 查询列
     */
    private String column;

    /**
     * 范围查询列表
     */
    private List<QueryRange> queryRanges;

    /**
     * 查询数值
     */
    private long search;

    /**
     * 查询字符串
     */
    private String searchKey;

    /**
     * 查询的key集合
     * 用于 like 时 and 或 or
     */
    private List<String> searchList;

    /*
     * 查询类型，默认是 eq
     */
    private int type;

    public String getColumn() {
        return column;
    }

    public ConditionSub setColumn(String column) {
        this.column = column;
        return this;
    }

    public List<QueryRange> getQueryRanges() {
        return queryRanges;
    }

    public ConditionSub setQueryRanges(List<QueryRange> queryRanges) {
        this.queryRanges = queryRanges;
        this.type = Constants.QUERY_TYPE_RANGE;
        return this;
    }

    public ConditionSub addQueryRange(QueryRange range) {
        if (this.queryRanges == null) {
            this.queryRanges = new ArrayList<QueryRange>();
        }
        this.queryRanges.add(range);
        this.type = Constants.QUERY_TYPE_RANGE;
        return this;
    }

    public long getSearch() {
        return search;
    }

    public int getType() {
        return type;
    }

    /**
     * 添加查询数值
     * @param search
     * @return
     */
    public ConditionSub setSearch(long search) {
        this.search = search;
        this.searchKey = null;
        this.type = Constants.QUERY_TYPE_EQUAL;
        return this;
    }

    /**
     * 添加查询数值
     * 目前只能用于非null字段
     * 对于null字段使用likeNot也查询不出来
     * @param search
     * @return
     */
    public ConditionSub setSearchNot(long search) {
        this.search = search;
        this.searchKey = null;
        this.type = Constants.QUERY_TYPE_NOT_EQUAL;
        return this;
    }

    /**
     * 添加查找字符串
     * @param search
     * @return
     */
    public ConditionSub setSearch(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_EQUAL;
        return this;
    }

    /**
     * 添加查找字符串
     * 目前只能用于非null字段
     * 对于null字段使用likeNot也查询不出来
     * @param search
     * @return
     */
    public ConditionSub setSearchNot(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_NOT_EQUAL;
        return this;
    }

    /**
     * 添加模糊匹配字段
     * @param search
     * @return
     */
    public ConditionSub setLike(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_LIKE;
        return this;
    }

    /**
     * 添加模糊匹配字段
     * 目前只能用于非null字段
     * 对于null字段使用likeNot也查询不出来
     * @param search
     * @return
     */
    public ConditionSub setLikeNot(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_NOT_LIKE;
        return this;
    }

    /**
     * 添加模糊匹配字段
     * @param search
     * @return
     */
    public ConditionSub setGrep(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_GREP;
        return this;
    }

    /**
     * 添加全文检索字段
     * @param search
     * @return
     */
    public ConditionSub setFulltextRetrieval(String search) {
        this.searchKey = search;
        this.search = 0;
        this.type = Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL;
        return this;
    }

    public String getSearchKey() {
        return searchKey;
    }

    public List<String> getSearchList() {
        return searchList;
    }

    public ConditionSub setLikeAnd(List<String> searchList) {
        this.searchList = searchList;
        this.type = Constants.QUERY_TYPE_KEY_LIKE_AND;
        return this;
    }

    public ConditionSub setLikeOr(List<String> searchList) {
        this.searchList = searchList;
        this.type = Constants.QUERY_TYPE_KEY_LIKE_OR;
        return this;
    }

    public ConditionSub setIn(List<String> searchList) {
        this.searchList = searchList;
        this.type = Constants.QUERY_TYPE_IN;
        return this;
    }

    @Override
    public String toString() {
        return "ConditionSub{" +
                "column='" + column + '\'' +
                ", queryRanges=" + queryRanges +
                ", search=" + search +
                ", searchKey='" + searchKey + '\'' +
                ", searchList='" + searchList + '\'' +
                ", type=" + type +
                '}';
    }
}
