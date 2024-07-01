package com.wuqing.business.bigstore.bean;

import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.util.Convert10To64;

/**
 * Created by wuqing on 17/9/5.
 */
public class LongCount {

    private long l;
    private int count = 0;
    private int lastLine = 0;
    private StringBuilder lines = new StringBuilder();

    public LongCount(long l, int line) {
        this.l = l;
        this.count = 1;
        lastLine = line;
        String ls = null;
        if (ServerConstants.USE_64) {
            ls = Convert10To64.compressNumber(line);
        } else {
            ls = String.valueOf(line);
        }
        lines.append(ls).append(",");
    }

    public long getL() {
        return l;
    }

    public int getCount() {
        return count;
    }

    public void addCount(int line) {
        this.count++;
        int lineRecord = line - lastLine;
        String ls = null;
        if (ServerConstants.USE_64) {
            ls = Convert10To64.compressNumber(lineRecord);
        } else {
            ls = String.valueOf(lineRecord);
        }
        lines.append(ls).append(ServerConstants.DESC_LINE_SPLIT);
        lastLine = line;
    }

    public String getLineNums() {
        if (lines.length() > 0) {
            return lines.substring(0, lines.length() - 1);
        } else {
            return "";
        }
    }




}
