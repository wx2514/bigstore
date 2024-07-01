package com.wuqing.client.bigstore.util;

import com.wuqing.client.bigstore.bean.sql.SelectTable;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.exception.SqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Created by wuqing on 18/5/30.
 */
public class SqlParse {

    private final static Logger logger = LoggerFactory.getLogger(SqlParse.class);

    private static final String SPLIT_KEY = "\t\n\r\f ";

    private static final String SELECT_KEY = Constants.SqlOperator.SELECT + " ";

    private static final String FROM_KEY = Constants.SqlOperator.FROM + " ";

    private static final String WHERE_KEY = Constants.SqlOperator.WHERE + " ";

    private static final String AND_KEY = "\t" + Constants.SqlOperator.AND + " ";

    private static final String LIMIT_KEY = Constants.SqlOperator.LIMIT + " ";

    private static final String GROUP_BY_KEY = Constants.SqlOperator.GROUP + " " + Constants.SqlOperator.BY + " ";

    private static final String AS_KEY = " " + Constants.SqlOperator.AS + " ";

    public static SelectTable convertQuerySql(String orgSql) {
        SelectTable selectTable = new SelectTable();
        //String sql = formatSqlByDruid(orgSql);
        String sql = formatSql(orgSql);
        String[] sqlArray = sql.split("\n");
        for (String s : sqlArray) {
            if (s.startsWith(SELECT_KEY)) {
                String[] arr = s.substring(SELECT_KEY.length()).split(",");
                for (String sel : arr) {
                    String[] selArray = sel.split(AS_KEY);
                    if (selArray.length == 1) {
                        selectTable.addSelFields(selArray[0]);
                    } else {
                        selectTable.addSelFields(selArray[0], selArray[1]);
                    }

                }
            } else if (s.startsWith(FROM_KEY)) {
                selectTable.setTable(s.substring(FROM_KEY.length()));
            } else if (s.startsWith(WHERE_KEY)) {
                selectTable.addWhere(s.substring(WHERE_KEY.length()));
            } else if (s.startsWith(AND_KEY)) {
                selectTable.addWhere(s.substring(AND_KEY.length()));
            } else if (s.startsWith(LIMIT_KEY)) {
                String[] arry = s.substring(LIMIT_KEY.length()).split(",");
                if (arry.length == 1) {
                    selectTable.setLimit(arry[0]);
                } else if (arry.length == 2) {
                    selectTable.setStart(arry[0]);
                    selectTable.setLimit(arry[1]);
                } else {
                    throw new SqlException("语法错误:" + s + "\n" + sql);
                }
            } else if (s.startsWith(GROUP_BY_KEY)) {
                String groupBy = s.replace(GROUP_BY_KEY, "").trim();
                selectTable.setGroupBy(groupBy);
            } else {
                throw new SqlException("SQL语法错误:" + s + "\n" + sql);
            }
        }
        //System.out.println(sql);
        return selectTable;
    }

    private static String formatSql(String orgSql) {
        orgSql = addBlank(orgSql);
        StringTokenizer st = new StringTokenizer(orgSql, SPLIT_KEY);
        int type = 0;   //初始值. 1:SELECT; 2:FROM; 3:WHERE; 4:AND; 5:BETWEEN,SEARCH,GREP,LICK; 6:LIMIT,7:GROUP; 8:BY; 9:AS; 10:NOT
        StringBuilder sb = new StringBuilder();
        while(st.hasMoreElements()) {
            String token = st.nextToken().trim();
            if (Constants.SqlOperator.SELECT.equals(token.toUpperCase())) {
                type = 1;
                sb.append(Constants.SqlOperator.SELECT);
                continue;
            }
            if (type == 0) {
                throw new SqlException("目前SQL只支持查询语句，\n" + orgSql);
            }
            if (Constants.SqlOperator.FROM.equals(token.toUpperCase())) {
                type = 2;
                sb.append("\n").append(Constants.SqlOperator.FROM);
                continue;
            }
            if (Constants.SqlOperator.WHERE.equals(token.toUpperCase())) {
                type = 3;
                sb.append("\n").append(Constants.SqlOperator.WHERE);
                continue;
            }
            if (Constants.SqlOperator.BETWEEN.equals(token.toUpperCase())) {
                type = 5;
                sb.append(" ").append(token.toUpperCase());
                continue;
            }
            if (Constants.SqlOperator.SEARCH.equals(token.toUpperCase())
                    || Constants.SqlOperator.GREP.equals(token.toUpperCase())
                    || Constants.SqlOperator.LIKE.equals(token.toUpperCase())
                    || Constants.SqlOperator.LIKE_OR.equals(token.toUpperCase())
                    || Constants.SqlOperator.LIKE_AND.equals(token.toUpperCase())
                    || Constants.SqlOperator.IN.equals(token.toUpperCase())) {
                type = 4;
                sb.append(" ").append(token.toUpperCase());
                continue;
            }
            if (Constants.SqlOperator.AND.equals(token.toUpperCase())) {
                if (type == 5) {
                    type = 4;
                    sb.append(" ").append(Constants.SqlOperator.AND);
                } else {
                    type = 4;
                    sb.append("\n\t").append(Constants.SqlOperator.AND);
                }
                continue;
            }
            if (Constants.SqlOperator.LIMIT.equals(token.toUpperCase())) {
                type = 6;
                sb.append("\n").append(Constants.SqlOperator.LIMIT);
                continue;
            }
            if (Constants.SqlOperator.GROUP.equals(token.toUpperCase())) {
                sb.append("\n").append(Constants.SqlOperator.GROUP);
                type = 7;
                continue;
            }
            if (Constants.SqlOperator.BY.equals(token.toUpperCase())) {
                sb.append(" ").append(Constants.SqlOperator.BY);
                type = 8;
                continue;
            }
            if (Constants.SqlOperator.AS.equals(token.toUpperCase())) {
                sb.append(" ").append(Constants.SqlOperator.AS);
                type = 9;
                continue;
            }
            if (Constants.SqlOperator.NOT.equals(token.toUpperCase())) {
                type = 10;
                sb.append(" ").append(token.toUpperCase());
                continue;
            }
            String formatStr = formatCondition(token);
            if (",".equals(formatStr)) {
                sb.append(formatStr);
            } else {
                sb.append(" ").append(formatStr);
            }


        }
        return sb.toString();
    }

    /**
     * 增加空格
     * 将 \n 替换成特殊字符
     * @param sql
     * @return
     */
    public static String addBlank(String sql) {
        int idx = sql.indexOf(" where ");
        if (idx == -1) {
            idx = sql.indexOf(" WHERE ");
        }
        char[] chars = sql.toCharArray();
        char[] charsNew = new char[chars.length * 2];
        int indexNew = 0;
        int mark = 0;
        for (char c : chars) {
            if (indexNew < idx || idx == -1) {   //where条件之前的直接赋值, 或者没找到where也直接赋值
                charsNew[indexNew++] = c;
                continue;
            }
            if (c == '\'') {
                mark++;
            }
            if (c == '\n' && mark % 2 == 1) {
                c = Constants.LINE_BREAK_REPLACE;  //括号内的回车
            }
            if ((c == '(') && mark % 2 == 0) {
                charsNew[indexNew++] = ' '; //补括号，避免类似likeor('') 的情况
                charsNew[indexNew++] = c;
            } else if ((c == ')') && mark % 2 == 0) {
                charsNew[indexNew++] = c;
                charsNew[indexNew++] = ' '; //补括号，避免类似likeor('') 的情况
            } else {
                charsNew[indexNew++] = c;
            }

        }
        return String.valueOf(Arrays.copyOf(charsNew, indexNew));
    }

    private static String formatCondition(String token) {   //对符号附近加空格
        char[] chars = token.toCharArray();
        int mark = 0;
        char[] charsNew = new char[chars.length * 2];
        int indexNew = 0;
        int idx = 0;
        char last = ' ';
        for (char c : chars) {
            if (c == '\'') {
                mark++;
            }
            if (c == '!' && mark % 2 == 0) {    //不在引号内
                if (idx > 0) { //不是第一个字符，才加前空格
                    charsNew[indexNew++] = ' ';
                }
                charsNew[indexNew++] = c;
            } else if (c == '=' && mark % 2 == 0) {    //不在引号内
                if (idx > 0 && last != '!') { //不是第一个字符，并且不是 != 才加前空格
                    charsNew[indexNew++] = ' ';
                }
                charsNew[indexNew++] = c;
                if (idx < chars.length - 1) {   //不是最后一个字符才加后空格
                    charsNew[indexNew++] = ' ';
                }
            } else if (c == ',' && mark % 2 == 0) {
                charsNew[indexNew++] = c;
                if (idx < chars.length - 1) {   //不是最后一个字符才加后空格
                    charsNew[indexNew++] = ' ';
                }
            } else {
                charsNew[indexNew++] = c;
            }
            idx++;
            last = c;
        }
        return String.valueOf(Arrays.copyOf(charsNew, indexNew));
    }


    /*private static String formatSqlByDruid(String orgSql) {
        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        MySqlStatementParser parser = new MySqlStatementParser(orgSql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            if (!(statement instanceof SQLSelectStatement)) {
                throw new SqlException("目前SQL只支持查询语句，\n" + orgSql);
            }
            statement.accept(visitor);
            visitor.println();
        }
        return out.toString();
    }*/

    public static void main(String[] args) {
        //String sql = "select a,b,* \t from   table   \t\r\n where \nname = 'wuqing'  and  old=    3 and price   between 50  and   100  and address search 'tianya'  limit 10 ,   10";
        //String sql = "select * from test_table where desc likeor '*描述0*','*描述1*' group by name limit 10";
        //String sql = "select scount(time_iso8601) from topic-elb-pro where host='ALL' group by status";
        //System.out.print(formatSqlByDruid(sql));
        //System.out.print(formatSql(sql));
        //String sql = "select smax(value) from flink_task where  type like '*jobmanager*' and type like '*Time*' group by key ";
        //String sql = "select scount(time_iso8601) as time_count from topic-elb-pro where upstream_addr_priv='ALL' and host='ALL' and status between 200 and 399  and url='ALL'  and elb_name likeor ('elb_e471a8ea-7fd0-460e-a9c9-11c295d32360','elb_0674c9c8-288d-4317-887c-0a4f4b649cfe') and status ='ALL' group by status";
        String sql = "select scount(time_iso8601) as time_count from topic-elb-pro where upstream_addr_priv='ALL' and status != '200' and status not like '*111*' group by status";
        SelectTable sel = convertQuerySql(sql);
        System.out.println(sel.toString());
    }

}
