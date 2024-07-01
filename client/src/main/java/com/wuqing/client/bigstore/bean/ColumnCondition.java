package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wuqing on 17/3/10.
 */
public class ColumnCondition implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据库
     */
    private String dataBase;

    /**
     * 表名
     */
    private String table;

    /**
     * 列名
     */
    private String column;

    /**
     * 查询起始行号
     */
    private int start;

    /**
     * 每次获取个数
     */
    private int limit;

    /**
     * 范围查询列表
     */
    private List<QueryRange> queryRanges;

    /**
     * 范围最小
     */
    //private long rangeMin = Long.MAX_VALUE;

    /**
     * 范围最大数值
     */
    //private long rangeMax = Long.MIN_VALUE;

    /**
     * 查询数值
     */
    private long search;

    /**
     * 查询的字符串
     */
    private String searchKey;

    /**
     * 查询的key
     */
    private List<String> searchList;

    /**
     * 查询类型 0:数值查询，1:范围查询
     */
    private int type;

    private Set<String> spaceDir = null;

    private List<DataPack> dataDirLsit = null;

    public ColumnCondition(String dataBase) {
        this.dataBase = dataBase;
    }

    public ColumnCondition setSearch(long search) {
        this.search = search;
        this.searchKey = null;
        return this;
    }

    public ColumnCondition setSearch(String search) {
        this.searchKey = search;
        if (search != null) {
            this.search = 0L;
        }
        return this;
    }

    /*public ColumnCondition setSearch(long start, long end) {
        this.searchStart = start;
        this.searchEnd = end;
        return this;
    }*/

    public int getStart() {
        return start;
    }

    public ColumnCondition setStart(int start) {
        this.start = start;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public ColumnCondition setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public String getDataBase() {
        return dataBase;
    }

    public ColumnCondition setDataBase(String dataBase) {
        this.dataBase = dataBase;
        return this;
    }

    public String getTable() {
        return table;
    }

    public ColumnCondition setTable(String table) {
        this.table = table;
        return this;
    }

    public String getColumn() {
        return column;
    }

    public ColumnCondition setColumn(String column) {
        this.column = column;
        return this;
    }

    public ColumnCondition setType(int type) {
        this.type = type;
        return this;
    }

    public long getSearch() {
        return search;
    }

    public String getSearchKey() {
        return this.searchKey;
    }

    public int getType() {
        return type;
    }

    public List<DataPack> getDataDirLsit() {
        return dataDirLsit;
    }

    public ColumnCondition setDataDirLsit(List<DataPack> dataDirLsit) {
        this.dataDirLsit = dataDirLsit;
        return this;
    }

    public List<QueryRange> getQueryRanges() {
        return queryRanges;
    }

    public ColumnCondition setQueryRanges(List<QueryRange> queryRanges) {
        this.queryRanges = queryRanges;
        return this;
    }

    public ColumnCondition addQueryRange(QueryRange range) {
        if (this.queryRanges == null) {
            this.queryRanges = new ArrayList<QueryRange>();
        }
        this.queryRanges.add(range);
        return this;
    }

    public List<String> getSearchList() {
        return searchList;
    }

    public ColumnCondition setSearchList(List<String> searchList) {
        this.searchList = searchList;
        return this;
    }

    public ColumnCondition copy() {
        return (new ColumnCondition(this.dataBase))
                .setTable(this.table)
                .setColumn(this.column)
                .setStart(this.start)
                .setLimit(this.limit)
                .setSearch(this.search)
                .setQueryRanges(this.getQueryRanges())
                .setSearchList(this.getSearchList());
    }

    public void calculateSpaceDir() {
        if (dataDirLsit == null) {
            spaceDir = null;
            return;
        }
        spaceDir = new HashSet<String>();
        DecimalFormat df = new DecimalFormat("0000000000");
        Set<String> set = new HashSet<String>();
        for (DataPack pack : dataDirLsit) {
            int spaceIdx = CommonUtil.parseInt(pack.getDirName()) / Constants.SPACE_SIZ;
            String spDir = df.format(spaceIdx);
            spaceDir.add(spDir);
        }
    }

    public Set<String> getSpaceDir() {
        return spaceDir;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(dataBase).append("=").append(table).append("=").append(column).append("=").append(type).append("=").append(search).append("=").append(searchKey).append("=").append(start).append("=").append(limit);
        if (queryRanges != null) {
            for (QueryRange r : queryRanges) {
                sb.append("=").append(r.getStart()).append("=").append(r.getEnd());
            }
        }
        return sb.toString();
    }

    public String  toCountKey() {
        return toCountKey(false);
    }

    public String toCountKey(boolean ignoreQueryRange) {
        StringBuilder sb = new StringBuilder();
        sb.append(dataBase).append("=").append(table).append("=").append(column).append("=").append(type).append("=").append(search).append("=").append(searchKey);
        if (!ignoreQueryRange) {
            if (queryRanges != null) {
                //TODO 通过这样的方式生成缓存key，存在冲突风险，这个跑一段时间观察一下，看看是否会遇到冲突的情况。（这样做是为了减少key长度节省缓存的内存）
                long total = 0L;
                int i = 0;
                for (QueryRange r : queryRanges) {
                    //sb.append("=").append(r.getStart()).append("=").append(r.getEnd());
                    int weiyi1 = 30 + (i++ % 5);
                    int weiyi2 = i++ % 5;
                    total += (r.getStart() << weiyi1) + r.getEnd() >> weiyi2;
                }
                sb.append("=").append(total);
            }
        }
        if (searchList != null) {
            for (String key : searchList) {
                sb.append("=").append(key);
            }
        }
        return sb.toString();
    }
}
