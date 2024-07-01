package com.wuqing.client.bigstore.bean.sql;

import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.exception.SqlException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 18/5/31.
 * 查询表格
 */
public class SelectTable {

    private String table;

    private List<String> selFields = new ArrayList<String>();

    private List<String> selFieldAs = new ArrayList<String>();

    private List<WhereCondition> where = new ArrayList<WhereCondition>();

    private Integer start;

    private Integer limit;

    private String groupBy;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        String tab = removeBackquote(table);
        if (tab.indexOf(" ") > -1 || tab.indexOf("\f") > -1
                || tab.indexOf("\r") > -1 || tab.indexOf("\n") > -1) {
            throw new SqlException("表名无效:" + tab);
        }
        this.table = tab;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public List<String> getSelFields() {
        return selFields;
    }

    public List<String> getSelFieldAs() {
        return selFieldAs;
    }

    public void addSelFields(String field) {
        this.addSelFields(field, null);
    }

    public void addSelFields(String field, String as) {
        field = removeBackquote(field);
        if (field != null && field.length() > 0 && !"*".equals(field)) {
            this.selFields.add(field);
            this.selFieldAs.add(as);
        }
    }

    public List<WhereCondition> getWhere() {
        return where;
    }

    public void addWhere(String whereStr) {
        int indexNotEq = whereStr.indexOf(Constants.SqlOperator.NOT_EQUAL);
        int indexEq = whereStr.indexOf(Constants.SqlOperator.EQUAL);
        int indexBetween = whereStr.indexOf(Constants.SqlOperator.BETWEEN);
        int indexSearch = whereStr.indexOf(Constants.SqlOperator.SEARCH);
        int indexGrep = whereStr.indexOf(Constants.SqlOperator.GREP);
        int indexNotLike = whereStr.indexOf(Constants.SqlOperator.NOT_LIKE);
        int indexLike = whereStr.indexOf(Constants.SqlOperator.LIKE);
        int indexLikeOr = whereStr.indexOf(Constants.SqlOperator.LIKE_OR);
        int indexLikeAnd = whereStr.indexOf(Constants.SqlOperator.LIKE_AND);
        int indexIn = whereStr.indexOf(Constants.SqlOperator.IN);
        if (indexNotEq > -1) {
            String key = removeBackquote(whereStr.substring(0, indexNotEq));
            String value = removeBackquote(whereStr.substring(indexNotEq + Constants.SqlOperator.NOT_EQUAL.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_NOT_EQUAL));
        } else if (indexEq > -1) {
            String key = removeBackquote(whereStr.substring(0, indexEq));
            String value = removeBackquote(whereStr.substring(indexEq + Constants.SqlOperator.EQUAL.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_EQUAL));
        } else if (indexBetween > -1) {
            String key = removeBackquote(whereStr.substring(0, indexBetween));
            String value = whereStr.substring(indexBetween + Constants.SqlOperator.BETWEEN.length());
            String[] arry = value.split(Constants.SqlOperator.AND);
            this.where.add(new WhereCondition(key, Long.parseLong(removeBackquote(arry[0])),
                    Long.parseLong(removeBackquote(arry[1]))));
        } else if (indexSearch > -1) {
            String key = removeBackquote(whereStr.substring(0, indexSearch));
            String value = removeBackquote(whereStr.substring(indexSearch + Constants.SqlOperator.SEARCH.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_SEARCH));
        } else if (indexGrep > -1) {
            String key = removeBackquote(whereStr.substring(0, indexGrep));
            String value = removeBackquote(whereStr.substring(indexGrep + Constants.SqlOperator.GREP.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_GREP));
        } else if (indexNotLike > -1) {
            String key = removeBackquote(whereStr.substring(0, indexNotLike));
            String value = removeBackquote(whereStr.substring(indexNotLike + Constants.SqlOperator.NOT_LIKE.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_NOT_LIKE));
        } else if (indexLikeOr > -1) {
            String key = removeBackquote(whereStr.substring(0, indexLikeOr));
            String vs = whereStr.substring(indexLikeOr + Constants.SqlOperator.LIKE_OR.length());
            List<String> values = split(vs);
            this.where.add(new WhereCondition(key, values, WhereCondition.QUERY_TYPE_KEY_LIKE_OR));
        } else if (indexLikeAnd > -1) {
            String key = removeBackquote(whereStr.substring(0, indexLikeAnd));
            String vs = whereStr.substring(indexLikeAnd + Constants.SqlOperator.LIKE_AND.length());
            List<String> values = split(vs);
            this.where.add(new WhereCondition(key, values, WhereCondition.QUERY_TYPE_KEY_LIKE_AND));
        } else if (indexLike > -1) {
            String key = removeBackquote(whereStr.substring(0, indexLike));
            String value = removeBackquote(whereStr.substring(indexLike + Constants.SqlOperator.LIKE.length()));
            this.where.add(new WhereCondition(key, value, WhereCondition.QUERY_TYPE_LIKE));
        } else if (indexIn > -1) {
            String key = removeBackquote(whereStr.substring(0, indexIn));
            String vs = whereStr.substring(indexIn + Constants.SqlOperator.IN.length());
            List<String> values = split(vs);
            this.where.add(new WhereCondition(key, values, WhereCondition.QUERY_TYPE_IN));
        } else {
            throw new SqlException("不支持的操作:" + whereStr);
        }
        //this.where.add();
    }

    private List<String> split(String str) {
        List<String> result = new ArrayList<>();
        str = str.trim();
        if (str.startsWith("(") && str.endsWith(")")) {
            str = str.substring(1, str.length() - 1);
        }
        List<Integer> splitIndex = new ArrayList<>();
        int count = 0;
        char[] chars = str.toCharArray();
        for (int i = 0, k = chars.length; i < k; i++) {
            char c = chars[i];
            if (c == '\'') {
                count++;
                continue;
            }
            if (count % 2 == 0 && c == ',') {
                splitIndex.add(i);
            }
        }
        int last = 0;
        for (Integer idx : splitIndex) {
            result.add(str.substring(last, idx).trim());
            last = idx + 1;
        }
        result.add(str.substring(last, str.length()).trim());
        return result;
    }

    private static String removeBackquote(String str) {
        if (str == null) {
            return str;
        }
        return str.replace("`", "").trim();
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = Integer.parseInt(removeBackquote(start));
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = Integer.parseInt(removeBackquote(limit));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT");
        String split = " ";
        for (int i = 0, k = this.selFields.size(); i < k; i++) {
            String f = this.selFields.get(i);
            sb.append(split).append(f);
            String as = this.selFieldAs.get(i);
            if (as != null) {
                sb.append(" AS ").append(as);
            }
            split = ", ";
        }
        sb.append("\nFROM ").append(this.table);
        split = "\nWHERE ";
        for (WhereCondition whr : where) {
            sb.append(split);
            sb.append(whr.getKey());
            if (whr.isRange()) {
                QueryRange range = (QueryRange) whr.getValue();
                sb.append(" BETWEEN ").append(range.getStart()).append(" AND ").append(range.getEnd());
            } else {
                if (whr.getValue() instanceof String) {
                    sb.append(" ").append(whr.getOperate()).append(" ").append("'").append(whr.getValue()).append("'");
                } else if (whr.getValue() instanceof List) {
                    sb.append(" ").append(whr.getOperate()).append(" ");
                    sb.append("(");
                    String flag = "";
                    for (Object s : (List) whr.getValue()) {
                        sb.append(flag).append("'").append(s).append("'");
                        flag = ",";
                    }
                    sb.append(")");
                } else {
                    sb.append(" ").append(whr.getOperate()).append(" ").append(whr.getValue());
                }
            }
            split = "\n\tAND ";
        }
        if (groupBy != null) {
            sb.append("\nGROUP BY ").append(groupBy);
        }
        if (limit != null) {
            sb.append("\nLIMIT ");
            if (start != null) {
                sb.append(start).append(", ");
            }
            sb.append(limit);
        }

        return  sb.toString();
    }

}
