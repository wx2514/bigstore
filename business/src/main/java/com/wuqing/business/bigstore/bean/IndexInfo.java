package com.wuqing.business.bigstore.bean;

import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.util.BloomUtil;
import com.wuqing.business.bigstore.util.BusinessUtil;
import com.wuqing.business.bigstore.util.Convert10To64;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.client.bigstore.bean.ColumnCondition;
import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.config.PackDescEnum;
import com.wuqing.client.bigstore.util.BloomFilter;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wuqing on 17/3/3.
 * 块索引文件
 */
public class IndexInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private int press = 0;
    private int count = 0;
    private int nullCount = 0;
    private long start;
    private long end;
    private BigDecimal sum = new BigDecimal(0);

    /**
     * 数字的 直方图 索引
     */
    private List<Index> indexes = new ArrayList<Index>();

    /**
     * 枚举的 索引
     */
    private Map<Integer, byte[]> enumIndexes = new HashMap<Integer, byte[]>();

    private byte[] keyword = null;

    private BloomFilter bloomFilter = null;

    /**
     * 字符的 位图 索引
     */
    private Map<Integer, String> charIndexes = new HashMap<Integer, String>();

    public IndexInfo(List<String> list) throws Exception {
        //long s = System.currentTimeMillis();
        if (CommonUtil.isEmpty(list)) {
            return;
        }
        int size = list.size();
        if (size >= 3) {
            press = CommonUtil.parseInt(list.get(PackDescEnum.PRESS.ordinal()));
            count = CommonUtil.parseInt(list.get(PackDescEnum.COUNT.ordinal()));
            String[] nullSum = list.get(PackDescEnum.NULL_COUNT_OTHERS.ordinal()).split(FileConfig.NULL_SUM_SPLIT);
            nullCount = CommonUtil.parseInt(nullSum[0]);
            if (nullSum.length >= 2) {
                sum = new BigDecimal(nullSum[1]);
            }
        }
        if (size >= 5) {
            start = CommonUtil.parseLong(list.get(PackDescEnum.START.ordinal()));
            end = CommonUtil.parseLong(list.get(PackDescEnum.END.ordinal()));
        }
        if (start == Long.MIN_VALUE) {   //如果开始时间是Long的最小值，说明这个是字符串
            for (int i = PackDescEnum.values().length, k = list.size(); i < k; i++) {
                String line = list.get(i);
                if (CommonUtil.isEmpty(line)) {
                    continue;
                }
                if (line.indexOf(FileConfig.BOLLM_TYPE_PATH) > -1) {
                    bloomFilter = BloomUtil.readBloom(line.substring(FileConfig.BOLLM_TYPE_LENGTH));
                    continue;
                }
                String[] array = line.split(FileConfig.INDEX_FLAG);
                if (array.length == 2) {    //字符位索引
                    charIndexes.put(CommonUtil.parseInt(array[0]), array[1]);
                } else if (array.length == 1) { //分词索引（摘要索引）
                    keyword = line.getBytes(Constants.DEFAULT_CHARSET);
                }
            }
        } else {    //剩下是 数值类型 或者 枚举类型
            for (int i = PackDescEnum.values().length, k = list.size(); i < k; i++) {
                String line = list.get(i);
                if (CommonUtil.isEmpty(line)) {
                    continue;
                }
                if (line.indexOf(FileConfig.INDEX_FLAG) == -1 || line.indexOf(FileConfig.INDEX_SPILT) == -1) {
                    continue;
                }
                int lastIndex = line.lastIndexOf(FileConfig.INDEX_FLAG);
                String[] array = new String[] {line.substring(0, lastIndex), line.substring(lastIndex + 1)};
                if (Constants.INDEX_NOT_EXSIT.equals(array[1])) {  //如果不存在则不记录，省点内存
                    continue;
                }
                String[] startEnd = array[0].split(FileConfig.INDEX_SPILT);
                if (startEnd.length == 2) { //数值类型的索引格式
                    long start = Long.parseLong(startEnd[0]);
                    long end = Long.parseLong(startEnd[1]);
                    indexes.add(new Index(start, end));
                } else if (startEnd.length == 1) {  //这样的格式就是枚举定义了，
                    enumIndexes.put(Integer.parseInt(startEnd[0]), SnappyUtil.compress(array[1].getBytes(Constants.DEFAULT_CHARSET)));
                }
            }
        }
        /*if (enumIndexes.size() > 0) {
            System.out.println("=time=:" + (System.currentTimeMillis() - s));
        }*/
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void setPress(int press) {
        this.press = press;
    }

    public int getPress() {
        return press;
    }

    public int getCount() {
        return count;
    }

    public int getNullCount() {
        return nullCount;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public boolean existEnumIndexes() {
        return !enumIndexes.isEmpty();
    }

    public boolean existKeywordIndexes() {
        return keyword != null && keyword.length > 0;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public static class Index {
        private long start;
        private long end;

        public Index(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public void setEnd(long end) {
            this.end = end;
        }
    }

    public int isExsit(ColumnCondition colCon) throws Exception {
        int res = Constants.RELATION_TAG_NONE; //假设不相关
        if (colCon.getType() == Constants.QUERY_TYPE_EQUAL) {
            if (colCon.getSearchKey() != null) {    //说明是文本检索
                res = isExsitForString(colCon.getSearchKey());
            } else {
                res = isExsit(colCon.getSearch(), colCon.getSearch());
            }
        } else if (colCon.getType() == Constants.QUERY_TYPE_RANGE) {
            List<QueryRange> ranges = colCon.getQueryRanges();
            if (ranges != null) {
                for (QueryRange r : ranges) {
                    res = Math.max(res, isExsit(r.getStart(), r.getEnd()));
                }
            }
        } else if (colCon.getType() == Constants.QUERY_TYPE_LIKE
                || colCon.getType() == Constants.QUERY_TYPE_GREP
                || colCon.getType() == Constants.QUERY_TYPE_KEY_LIKE_AND
                || colCon.getType() == Constants.QUERY_TYPE_KEY_LIKE_OR
                || colCon.getType() == Constants.QUERY_TYPE_NOT_EQUAL
                || colCon.getType() == Constants.QUERY_TYPE_NOT_LIKE) {
            res = Constants.RELATION_TAG_MAYBE;
        } else if (colCon.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL) {
            res = isKeyExsit(colCon.getSearchKey());
        } else if (colCon.getType() == Constants.QUERY_TYPE_IN) {
            boolean isString = start == Long.MIN_VALUE;   //如果开始时间是Long的最小值，说明这个是字符串
            for (String key : colCon.getSearchList()) {
                if (isString) {
                    res = Math.max(res, isExsitForString(key));
                } else {
                    Long l = CommonUtil.parseLong2(key);
                    if (l != null) {
                        res = Math.max(res, isExsit(l, l));
                    }
                }
            }
        }
        return res;
    }

    public int isExsitForString(String searchKey) throws Exception {
        if (bloomFilter != null) {
            if (bloomFilter.exist(searchKey)) {
                //如果存在返回可疑相关
                return Constants.RELATION_TAG_MAYBE;
            } else {
                //否则返回不相关
                return Constants.RELATION_TAG_NONE;
            }
        }
        if (CommonUtil.isEmpty(charIndexes)) {  //如果是空的，说明没有索引，只能返回可能相关
            return Constants.RELATION_TAG_MAYBE;
        }
        byte[] bts = searchKey.getBytes(Constants.DEFAULT_CHARSET);
        for (int i = 0, k = Math.min(bts.length, Constants.STRING_INDEX_COUNT); i < k; i++) {
            int ascii = bts[i];
            String index = charIndexes.get(ascii);
            if (index == null || index.indexOf(FileConfig.INDEX_SPILT + i + FileConfig.INDEX_SPILT) == -1) {    //如果索引没命中，则返回不相关
                return Constants.RELATION_TAG_NONE; //直接返回不相关
            }
        }
        return Constants.RELATION_TAG_MAYBE;
    }

    /**
     * 分词模式下的 索引关键字存在判定
     * @param word
     * @return
     * @throws Exception
     */
    public int isKeyExsit(String word) throws Exception {
        byte[] wordByte = BusinessUtil.getHashCodeStr(word).getBytes(Constants.DEFAULT_CHARSET);
        if (keyword == null) {  //如果没有关键词索引信息那么只能判定为 可疑相关
            return Constants.RELATION_TAG_MAYBE;
        }
        int wordLength = wordByte.length;
        for (int i = 0, k = keyword.length; i < k; i += wordLength) {
            boolean match = true;
            for (int j = 0; j < wordLength; j++) {
                match &= (keyword[i + j] == wordByte[j]);
                if (!match) {
                    break;
                }
            }
            if (match) {    //如果匹配到了，说明有数据，判定可疑相关
                return Constants.RELATION_TAG_MAYBE;
            }
        }
        return Constants.RELATION_TAG_NONE; //如果没有找到，则说明不相关
    }

    public int isExsit(long dataStart, long dataEnd) {
        if (dataStart <= this.start && dataEnd >= this.end) {
            if (nullCount == 0) {
                return Constants.RELATION_TAG_ALL;
            } else {
                return Constants.RELATION_TAG_MAYBE;
            }
        } else if (dataStart <= this.end && dataEnd >= this.start) {
            if (this.indexes != null && !this.indexes.isEmpty()) {
                for (Index index : this.indexes) {
                    if (dataStart <= index.end && dataEnd >= index.start) {
                        return Constants.RELATION_TAG_MAYBE;
                    }
                }
            } else if (this.existEnumIndexes() && dataStart == dataEnd) {
                if (this.enumIndexes.get((int) dataStart) != null) {
                    return Constants.RELATION_TAG_MAYBE;
                }
            } else {    //没有位图索引，则不判定
                return Constants.RELATION_TAG_MAYBE;
            }

        }
        return Constants.RELATION_TAG_NONE;
    }

    public boolean isPress() {
        return press > 0;
    }

    /**
     * 外围需要判定是否是枚举类型, 这里就不做验证了
     * @param colCon
     * @return
     * @throws Exception
     */
    public int[] getLineNums(ColumnCondition colCon) throws Exception {
        if (colCon.getType() == Constants.QUERY_TYPE_EQUAL) {
            byte[] bts = SnappyUtil.decompress(enumIndexes.get((int) colCon.getSearch()));
            if (bts == null) {
                return new int[0];
            }
            String str = new String(bts, Constants.DEFAULT_CHARSET);
            String[] ls = str.split(ServerConstants.DESC_LINE_SPLIT);
            int[] ils = new int[ls.length];
            for (int i = 0, k = ls.length; i < k; i++) {
                if (ServerConstants.USE_64) {
                    ils[i] = (int) Convert10To64.unCompressNumberByLine(ls[i]);
                } else {
                    ils[i] = Integer.parseInt(ls[i]);
                }
            }
            return ils;
        }
        return new int[0];
    }

    public boolean isFull() {
        return getCount() == ServerConstants.PARK_SIZ;
    }

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        List<String> lines = FileUtil.readAll("/Users/wuqing/sysOriginMsg_desc.txt", false);
        System.out.println(System.currentTimeMillis() - s);
        IndexInfo indexInfo = new IndexInfo(lines);
        int res = indexInfo.isKeyExsit("8763985345");
        System.out.println(res);
    }

}
