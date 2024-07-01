package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.bean.sql.SelectTable;
import com.wuqing.client.bigstore.bean.sql.WhereCondition;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.exception.SqlException;
import com.wuqing.client.bigstore.util.SqlParse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/15.
 */
public class Condition implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;


    public static final int DEFAULT_LIMIT = 10000;

    private boolean ignoreCount = false;

    /**
     * 执行的SQL
     */
    private String sql;

    /**
     * 数据库
     */
    private String dataBase;

    /**
     * 表名
     */
    private String table;

    /**
     * 查询起始行号
     */
    private int start;

    /**
     * 每次获取个数, 默认10000
     */
    private int limit = DEFAULT_LIMIT;

    /**
     * 只对按秒聚合有效
     * 单台机器获取的count如果超过这个数值，就直接抛异常而不进行聚合操作
     * 集群中如果有一个机器触发这个条件抛异常，就会返回错误
     */
    private int oneMachineAggregationLimit = 0;

    /**
     * group分组字段
     * 暂时 只能用于 按秒统计分组
     */
    private String groupBy;

    /**
     * select 的字段
     */
    private List<String> fields = new ArrayList<String>();

    /**
     * as 的别名字段
     */
    private List<String> fieldAs = new ArrayList<String>();

    /**
     * 特殊字段
     */
    List<FieldAs> fieldAsSpe = new ArrayList<>();

    /**
     * 根据ID查询, 优先级最高，如果有此查询，别的查询都将无效
     */
    private List<Long> id = new ArrayList<Long>();

    /**
     * 子条件列表
     */
    List<ConditionSub> conditionSubList = new ArrayList<ConditionSub>();

    /**
     * 子条件列表
     */
    List<ConditionSub> conditionSubFilter = new ArrayList<ConditionSub>();

    public Condition(String dataBase) {
        this.dataBase = dataBase;
    }

    public Condition() {
    }

    public void setSql(String sql) {
        SelectTable selectTable = SqlParse.convertQuerySql(sql);
        if (selectTable.getTable() == null) {
            throw new SqlException("语法错误,找不到表名 \n" + sql);
        }
        this.sql = selectTable.toString();
        this.setTable(selectTable.getTable());
        if (selectTable.getStart() != null) {
            this.setStart(selectTable.getStart());
        }
        if (selectTable.getLimit() != null) {
            this.setLimit(selectTable.getLimit());
        }
        if (selectTable.getGroupBy() != null) {
            this.setGroupBy(selectTable.getGroupBy());
        }
        for (int i = 0, k = selectTable.getSelFields().size(); i < k; i++) {
            String f = selectTable.getSelFields().get(i);
            String as = selectTable.getSelFieldAs().get(i);
            this.addField(f, as);
        }
        for (WhereCondition whr : selectTable.getWhere()) {
            ConditionSub sub = new ConditionSub().setColumn(whr.getKey());
            if (whr.isEqual()) {
                if (whr.getValue() instanceof Long) {
                    sub.setSearch((Long) whr.getValue());
                } else if (whr.getValue() instanceof String) {
                    sub.setSearch((String) whr.getValue());
                }
            } else if (whr.isNotEqual()) {
                if (whr.getValue() instanceof Long) {
                    sub.setSearchNot((Long) whr.getValue());
                } else if (whr.getValue() instanceof String) {
                    sub.setSearchNot((String) whr.getValue());
                }
            } else if (whr.isSearch()) {
                sub.setFulltextRetrieval((String) whr.getValue());
            } else if (whr.isRange()) {
                sub.addQueryRange((QueryRange) whr.getValue());
            } else if (whr.isGrep()) {
                sub.setGrep((String) whr.getValue());
            } else if (whr.isLike()) {
                sub.setLike((String) whr.getValue());
            } else if (whr.isLikeOr()) {
                sub.setLikeOr((List<String>) whr.getValue());
            } else if (whr.isLikeAnd()) {
                sub.setLikeAnd((List<String>) whr.getValue());
            } else if (whr.isNotLike()) {
                sub.setLikeNot((String) whr.getValue());
            } else if (whr.isIn()) {
                sub.setIn((List<String>) whr.getValue());
            }
            this.addConditionSubList(sub);
        }

        int size = this.getFields().size();
        List<Integer> remove = new ArrayList<>();
        List<FieldAs> fieldAsSpe = new ArrayList<>();
        int nowSpeIdx = 0;
        for (int i = 0; i < size; i++) {
            String fd = this.getFields().get(i);
            String as = this.getFieldAs().get(i);
            if (Constants.SPECIAL_FEILD.contains(fd.toUpperCase())) {
                remove.add(i);
                fieldAsSpe.add(new FieldAs(fd, as));
                if (i == nowSpeIdx) {
                    nowSpeIdx++;
                } else {
                    throw new SqlException("语法错误,特殊字段 " + fd +  " 必须放在前面\n" + sql);
                }
            }
        }
        for (int i = remove.size() - 1; i >= 0; i--) {
            int idx = remove.get(i);
            this.getFields().remove(idx);
            this.getFieldAs().remove(idx);
        }
        //特殊字段
        this.setFieldAsSpe(fieldAsSpe);
    }

    public String getDataBase() {
        return dataBase;
    }

    public Condition setDataBase(String dataBase) {
        this.dataBase = dataBase;
        return this;
    }

    public String getTable() {
        return table;
    }

    public Condition setTable(String table) {
        this.table = table;
        return this;
    }

    public int getStart() {
        return start;
    }

    public Condition setStart(int start) {
        this.start = start;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public Condition setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<String> getFieldAs() {
        return fieldAs;
    }

    public List<FieldAs> getFieldAsSpe() {
        return fieldAsSpe;
    }

    public void setFieldAsSpe(List<FieldAs> fieldAsSpe) {
        this.fieldAsSpe = fieldAsSpe;
    }

    public Condition addField(String field, String as) {
        this.fields.add(field);
        this.fieldAs.add(as);
        return this;
    }

    public Condition addField(String field) {
        this.fields.add(field);
        this.fieldAs.add(null);
        return this;
    }

    public int getOneMachineAggregationLimit() {
        return oneMachineAggregationLimit;
    }

    public void setOneMachineAggregationLimit(int oneMachineAggregationLimit) {
        this.oneMachineAggregationLimit = oneMachineAggregationLimit;
    }

    public List<ConditionSub> getConditionSubList() {
        return conditionSubList;
    }

    public List<ConditionSub> getConditionSubFilter() {
        return conditionSubFilter;
    }

    public void addConditionSubList(ConditionSub sub) {
        this.conditionSubList.add(sub);
    }

    public void addConditionSubFilter(ConditionSub filter) {
        this.conditionSubFilter.add(filter);
    }

    public Condition addId(long id) {
        this.id.add(id);
        return this;
    }

    public List<Long> getId() {
        return id;
    }

    public List<ColumnCondition> toColumnConditions() {
        List<ColumnCondition> colConList = new ArrayList<ColumnCondition>();
        for (ConditionSub sub : conditionSubList) {
            ColumnCondition columnCon = new ColumnCondition(this.dataBase);
            columnCon.setTable(this.table)
                    .setStart(this.start)
                    .setLimit(this.limit)
                    .setColumn(sub.getColumn())
                    .setSearch(sub.getSearch())
                    .setSearch(sub.getSearchKey())
                    .setQueryRanges(sub.getQueryRanges())
                    .setSearchList(sub.getSearchList())
                    .setType(sub.getType());
            colConList.add(columnCon);
        }
        return colConList;
    }

    public boolean isIgnoreCount() {
        return ignoreCount;
    }

    public void setIgnoreCount(boolean ignoreCount) {
        this.ignoreCount = ignoreCount;
    }

    @Override
    public Object clone() {   //浅拷贝
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "Condition{" +
                "dataBase='" + dataBase + '\'' +
                ", table='" + table + '\'' +
                ", start=" + start +
                ", limit=" + limit +
                ", conditionSubList=" + conditionSubList +
                ", id=" + id +
                ", oneMachineAggregationLimit=" + oneMachineAggregationLimit +
                ", groupBy=" + groupBy +
                '}';
    }

    public static void main(String[] args) {
        //String sql = "select a,b,* \t from   table   \t\r\n where \nname = 'wuqing'  and  old=    3 and price   between 50  and   100  and address='tianya'  limit 10 ,   10";
        //String sql = "select * from test_table where desc likeor '*描述0*','*描述1*' limit 10";
        String sql = "select smax(value) from flink_task where time between 100 and 999 and type != 'sss' and type not  like '*111*'  group by key ";
        Condition c = new Condition();
        c.setSql(sql);
        System.out.println(c);
    }
}
