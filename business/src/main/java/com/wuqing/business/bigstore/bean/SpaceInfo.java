package com.wuqing.business.bigstore.bean;

import com.wuqing.client.bigstore.bean.ColumnCondition;
import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.util.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/15.
 * 表分区信息
 */
public class SpaceInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = LoggerFactory.getLogger(SpaceInfo.class);

    /**
     * 最小值
     */
    private long min;

    /**
     * 最大值
     */
    private long max;

    /**
     * 数字的 直方图 索引
     */
    private List<IndexInfo.Index> indexes = new ArrayList<IndexInfo.Index>();

    private BloomFilter bloomFilter = null;

    public SpaceInfo(BloomFilter filter) {
        this.bloomFilter = filter;
    }

    public SpaceInfo(long min, long max, List<String> indexLines) {
        this.min = min;
        this.max = max;
        for (String line :indexLines) {
            int lastIndex = line.lastIndexOf(FileConfig.INDEX_FLAG);
            String[] array = new String[] {line.substring(0, lastIndex), line.substring(lastIndex + 1)};
            //如果不存在则不记录，省点内存
            if (Constants.INDEX_NOT_EXSIT.equals(array[1])) {
                continue;
            }
            String[] startEnd = array[0].split(FileConfig.INDEX_SPILT);
            //数值类型的索引格式
            if (startEnd.length == 2) {
                long start = Long.parseLong(startEnd[0]);
                long end = Long.parseLong(startEnd[1]);
                indexes.add(new IndexInfo.Index(start, end));
            }
        }

    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public int isExsit(ColumnCondition colCon) {
        //假设不相关
        int res = Constants.RELATION_TAG_NONE;
        if (colCon.getType() == Constants.QUERY_TYPE_EQUAL) {
            if (colCon.getSearchKey() != null) {
                //字符串分区索引列
                res = isExsit(colCon.getSearchKey());
            } else {
                res = isExsit(colCon.getSearch());
            }
        } else if (colCon.getType() == Constants.QUERY_TYPE_RANGE) {
            List<QueryRange> ranges = colCon.getQueryRanges();
            if (ranges != null) {
                for (QueryRange r : ranges) {
                    res = Math.max(res, isExsit(r.getStart(), r.getEnd()));
                }
            }
        } else {    //分区索引理论上不可能存在下面的情况，如果有先判定成可疑相关
            LOGGER.error("column type is invalid. type:" + colCon.getType());
            res = Constants.RELATION_TAG_MAYBE;
        }
        return res;
    }

    public int isExsit(String data) {
        //如果没有布隆过滤器，则无法判断，只能给可疑相关
        if (bloomFilter == null) {
            return Constants.RELATION_TAG_MAYBE;
        }
        boolean exist = bloomFilter.exist(data);
        if (exist) {
            //如果判定存在，那就是可能存在，可能不存在
            return Constants.RELATION_TAG_MAYBE;
        } else {
            //如果判定不存在，那就是不存在
            return Constants.RELATION_TAG_NONE;
        }
    }

    public int isExsit(long data) {
        return isExsit(data, data);
    }

    public int isExsit(long dataStart, long dataEnd) {
        if (dataStart <= this.min && dataEnd >= this.max) {
            return Constants.RELATION_TAG_ALL;
        } else if (dataStart <= this.max && dataEnd >= this.min) {
            if (this.indexes != null && !this.indexes.isEmpty()) {
                for (IndexInfo.Index index : this.indexes) {
                    if (dataStart <= index.getEnd() && dataEnd >= index.getStart()) {
                        return Constants.RELATION_TAG_MAYBE;
                    }
                }
            } else {
                return Constants.RELATION_TAG_MAYBE;
            }
        }
        return Constants.RELATION_TAG_NONE;
    }

}
