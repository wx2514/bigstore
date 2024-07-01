package com.wuqing.tool.bigstore.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/1/9.
 */
public class ConsoleTable {

    /**
     * 计算字符占位长度所用的字符集
     */
    private static final String CALCULATE_LENGTH_CHARSET = "GBK";

    private List<List> rows = new ArrayList<List>();

    private int colum;

    private int[] columLen;

    private static int margin = 2;

    private boolean printHeader = false;

    public ConsoleTable(int colum, boolean printHeader) {
        this.printHeader = printHeader;
        this.colum = colum;
        this.columLen = new int[colum];
    }

    public void appendRow(Object... values) {
        List row = new ArrayList(colum);
        rows.add(row);
        for (Object o : values) {
            this.appendColum(o);
        }
    }

    public void appendRow(List values) {
        List row = new ArrayList(colum);
        rows.add(row);
        for (Object o : values) {
            this.appendColum(o);
        }
    }

    private ConsoleTable appendColum(Object value) {
        if (value == null) {
            value = "NULL";
        }
        List row = rows.get(rows.size() - 1);
        row.add(value);
        int len = 0;
        try {
            len = value.toString().getBytes(CALCULATE_LENGTH_CHARSET).length;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (columLen[row.size() - 1] < len) {
            columLen[row.size() - 1] = len;
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        int sumlen = 0;
        for (int len : columLen) {
            sumlen += len;
        }
        if (printHeader) {
            buf.append("|").append(printChar('=', sumlen + margin * 2 * colum + (colum - 1))).append("|\n");
        }
        else {
            //buf.append("|").append(printChar('-', sumlen + margin * 2 * colum + (colum - 1))).append("|\n");
            if (rows.size() > 0) {
                rows.remove(0); //将头部去掉不打印
            }
        }

        for (int ii = 0; ii < rows.size(); ii++) {
            List row = rows.get(ii);
            for (int i = 0; i < colum; i++) {
                String o = "";
                if (i < row.size()) {
                    o = row.get(i).toString();
                }
                buf.append('|').append(printChar(' ', margin)).append(o);
                int len = 0;
                try {
                    len = o.getBytes(CALCULATE_LENGTH_CHARSET).length;
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                buf.append(printChar(' ', columLen[i] - len + margin));
            }
            buf.append("|\n");
            if (printHeader && ii == 0) {
                buf.append("|").append(printChar('=', sumlen + margin * 2 * colum + (colum - 1))).append("|\n");
            } else {
                buf.append("|").append(printChar('-', sumlen + margin * 2 * colum + (colum - 1))).append("|\n");
            }

        }
        return buf.toString();
    }

    private String printChar(char c, int len) {
        StringBuilder buf = new StringBuilder();
        if (c == '-' || c == '=') {
            len = Math.min(len, 150);
        }
        for (int i = 0; i < len; i++) {
            buf.append(c);
        }
        return buf.toString();
    }

    public static void main(String[] args) {
        /*ConsoleTable t = new ConsoleTable(4, true);
        t.appendRow();
        t.appendColum("Number").appendColum("Name").appendColum("Sex").appendColum("Old");
        t.appendRow();
        t.appendColum("1").appendColum("sdfdsfdsfdsfdfsdfdfdfsfsf").appendColum("man").appendColum("11");
        t.appendRow();
        t.appendColum("22").appendColum("erer343443111");
        System.out.println(t.toString());*/
    }


}
