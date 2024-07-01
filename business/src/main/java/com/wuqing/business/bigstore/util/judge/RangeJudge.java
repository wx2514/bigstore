package com.wuqing.business.bigstore.util.judge;

import com.wuqing.business.bigstore.util.DataBaseUtil;
import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.bean.QueryRangeByte;
import com.wuqing.client.bigstore.config.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/27.
 */
public class RangeJudge implements Judge {

    private List<QueryRangeByte> rangeBytes = null;


    public RangeJudge(List<QueryRange> queryRange, int length) throws Exception {
        if (queryRange == null) {
            rangeBytes = new ArrayList<QueryRangeByte>(0);
            return;
        }
        rangeBytes = new ArrayList<QueryRangeByte>(queryRange.size());
        for (QueryRange range : queryRange) {
            rangeBytes.add(new QueryRangeByte(DataBaseUtil.formatData(range.getStart(), length, true).getBytes(Constants.DEFAULT_CHARSET),
                    DataBaseUtil.formatData(range.getEnd(), length, true).getBytes(Constants.DEFAULT_CHARSET)));
        }
    }

    @Override
    public boolean isMath(byte[] key) {
        if (rangeBytes != null) {
            for (QueryRangeByte qr : rangeBytes) {
                if (DataBaseUtil.compare(key, qr.getStart(), true) >= 0 && DataBaseUtil.compare(key, qr.getEnd(), true) <= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /*private int indexOf(byte[] source, byte[] target) {
        return indexOf(source, target);
    }*/

    /**
     * 例如 source:aabbcc     target:bb
     * @param source 源数据
     * @param target 需要查找到的匹配数据
     * @param fromIndex 起始查询的位置
     * @return
     */
    private int indexOf(byte[] source, byte[] target, int fromIndex) {
        int sourceCount = source.length;
        int sourceOffset = 0;
        int targetOffset = 0;
        int targetCount = target.length;
        if (targetCount == 0) {
            return fromIndex;
        }
        byte first = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);
        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first byte. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first) {};
            }

            /* Found first byte, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++) {};

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    /**
     * 找到最后一个位置
     * @param source 源数据
     * @param target 需要查找到的匹配数据
     * @param fromIndex 起始查询的位置
     * @return
     */
    private int lastIndexOf(byte[] source, byte[] target, int fromIndex) {
        int sourceCount = source.length;
        int targetCount = target.length;
        int sourceOffset = 0;
        int targetOffset = 0;
        int rightIndex = sourceCount - targetCount;
        if (fromIndex < 0) {
            return -1;
        }
        if (fromIndex > rightIndex) {
            fromIndex = rightIndex;
        }
        /* Empty string always matches. */
        if (targetCount == 0) {
            return fromIndex;
        }

        int strLastIndex = targetOffset + targetCount - 1;
        byte strLastChar = target[strLastIndex];
        int min = sourceOffset + targetCount - 1;
        int i = min + fromIndex;

        startSearchForLastChar:
        while (true) {
            while (i >= min && source[i] != strLastChar) {
                i--;
            }
            if (i < min) {
                return -1;
            }
            int j = i - 1;
            int start = j - (targetCount - 1);
            int k = strLastIndex - 1;

            while (j > start) {
                if (source[j--] != target[k--]) {
                    i--;
                    continue startSearchForLastChar;
                }
            }
            return start - sourceOffset + 1;
        }
    }

}
