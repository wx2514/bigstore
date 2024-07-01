package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.bean.TableInfo;
import com.wuqing.business.bigstore.config.LongLengthMapping;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.util.judge.*;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.business.bigstore.util.judge.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wuqing on 17/3/9.
 */
public class ByteDataUtil {

    private final static Logger logger = LoggerFactory.getLogger(ByteDataUtil.class);

    private final static Logger queryDetailLogger = LoggerFactory.getLogger("query-detail-log");

    /**
     *
     * @param bts
     * @param condition
     * @param dir
     * @param getData true:数据(data)查询，false:数量(count)查询
     * @return
     * @throws Exception
     */
    public static QueryResult query(byte[] bts, ColumnCondition condition, DataPack dir, boolean getData) throws Exception {
        //long s = System.currentTimeMillis();
        List<DataLine> result = null;
        List<LineRange> lineRangeList = null;
        if (getData) {
            result = new ArrayList<DataLine>();
        } else {
            lineRangeList = new ArrayList<LineRange>();
        }
        if (bts == null) {
            return new QueryResult(0, dir).setData(result).setLineRangeList(lineRangeList);
        }
        //特殊查询方式 grep
        if (condition.getType() == Constants.QUERY_TYPE_GREP
                || condition.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL) {    //全文检索如果泡到这里了，说明还没有生成索引文件，那么也走grep模式吧
            return queryByGrep(bts, condition, dir, getData);
        }

        long startNum = CommonUtil.parseInt(dir.getDirName()) * ServerConstants.PARK_SIZ;
        TableInfo tableInfo = CacheUtil.readTableInfo(condition.getDataBase(), condition.getTable());
        ColumnDef def = tableInfo.getColumnDef(condition.getColumn());
        int length = def == null ? 0 : def.getLength();
        Judge rangeJudge = null;
        if (def.isLong() || def.isEnum()) {
            long sech = condition.getSearch();
            if (condition.getType() == Constants.QUERY_TYPE_EQUAL) {
                rangeJudge = new EqualJudge(sech, length);
            } else if (condition.getType() == Constants.QUERY_TYPE_RANGE) {
                rangeJudge = new RangeJudge(condition.getQueryRanges(), length);
            } else if (condition.getType() == Constants.QUERY_TYPE_NOT_EQUAL) {
                rangeJudge = new NotEqualJudge(sech, length);
            } else if (condition.getType() == Constants.QUERY_TYPE_IN) {
                long[] searchs = new long[condition.getSearchList().size()];
                int k = 0;
                for (String key : condition.getSearchList()) {
                    searchs[k++] = CommonUtil.parseLong(key, Long.MIN_VALUE);
                }
                rangeJudge = new InJudge(length, searchs);
            }
        } else {
            if (condition.getType() == Constants.QUERY_TYPE_LIKE) { //模糊匹配
                rangeJudge = new LikeJudge(condition.getSearchKey());
            } else if (condition.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL) {  //这个分支目前不会被走到(因为前面会被return)，为了未来可能被调用到，先保证正确性
                rangeJudge = new LikeJudge("*" + condition.getSearchKey() + "*");
            } else if (condition.getType() == Constants.QUERY_TYPE_KEY_LIKE_AND) {
                rangeJudge = new AndLikeJudge(condition.getSearchList());
            } else if (condition.getType() == Constants.QUERY_TYPE_KEY_LIKE_OR) {
                rangeJudge = new OrLikeJudge(condition.getSearchList());
            } else if (condition.getType() == Constants.QUERY_TYPE_NOT_EQUAL) {
                rangeJudge = new NotEqualJudge(condition.getSearchKey(), length);
            } else if (condition.getType() == Constants.QUERY_TYPE_NOT_LIKE) {
                rangeJudge = new NotLikeJudge(condition.getSearchKey());
            } else if (condition.getType() == Constants.QUERY_TYPE_IN) {
                String[] searchs = new String[condition.getSearchList().size()];
                int k = 0;
                for (String key : condition.getSearchList()) {
                    searchs[k++] = key;
                }
                rangeJudge = new InJudge(length, searchs);
            } else {
                rangeJudge = new EqualJudge(condition.getSearchKey(), length);
            }
        }

        long total = 0L;
        int numStart = 0;
        int numEnd = 0;
        int dataStart = 0;
        //boolean startMath = false;  //匹配开启标识
        //int math = 0;  //假设是匹配的, 0:代表开始匹配，-1:代表停止匹配，>0:代表匹配的个数
        //int mathcIndex = 0; //匹配的字节下标
        int dataIndex = 0;
        int commonSeg = 1;
        ColumnDef numDef = tableInfo.getColumnDef(Constants.COLUMN_ID);
        int lineSeg = numDef.getLength() + 1;
        int numSeg = def.getLength() + 1;
        if (ServerConstants.USE_64) {
            lineSeg = LongLengthMapping.get64By10(lineSeg - 1) + 1;
            if (def.isLong() || def.isEnum()) {
                numSeg = LongLengthMapping.get64By10(numSeg - 1) + 1;
            }
        }
        for (int i = 0, k = bts.length; i < k;) {
            if (bts[i] == Constants.LINE_SEPARATOR) {
                //startMath = false;
                byte[] dataByte = Arrays.copyOfRange(bts, dataStart, i);
                if (rangeJudge.isMath(dataByte)) {
                    dataIndex++;
                    //计算行号，以及完整的ID
                    byte[] lineNumByte = Arrays.copyOfRange(bts, numStart, numEnd);
                    int lineNum = 0;
                    if (ServerConstants.USE_64) {
                        lineNum = (int) Convert10To64.unCompressNumberByLine(new String(lineNumByte, Constants.DEFAULT_CHARSET));
                    } else {
                        lineNum = Integer.parseInt(new String(lineNumByte, Constants.DEFAULT_CHARSET));
                    }
                    long id = startNum + lineNum;
                    if (getData) {  //数据查询, 追加数据, 不过这个分支目前跑不到，因为拿数据都是根据ID去获取的
                        if (condition.getLimit() > 0 && result.size() < condition.getLimit()
                                && dataIndex > condition.getStart()) {
                            result.add(new DataLine(new String(bts, dataStart, i - dataStart, Constants.DEFAULT_CHARSET), id));
                        }
                    } else { //count查询, 追加id
                        int size = lineRangeList.size();
                        if (size > 0) {
                            LineRange rangeLast = lineRangeList.get(size - 1);
                            long last = rangeLast.getEnd();
                            if (id - last == 1) {   //连续的
                                rangeLast.setEnd(id);
                            } else {
                                lineRangeList.add(new LineRange(id));
                            }
                        } else {
                            lineRangeList.add(new LineRange(id));
                        }
                    }
                    total++;
                }
                numStart = i + 1;
                i += lineSeg;
            } else if (bts[i] == Constants.STACK_SPLIT) {
                numEnd = i;
                dataStart = i + 1;
                //startMath = true;
                //mathcIndex = 0;
                //math = 0;  //重置匹配标签
                i += numSeg;
            } else {
                /*if (startMath && math >= 0) {
                    if (mathcIndex < search.length) {
                        if (bts[i] != search[mathcIndex++]) {
                            math = -1;
                        } else {
                            math++;
                        }
                    } else {
                        math = -1;
                    }
                }*/
                i += commonSeg;
            }
        }
        //queryDetailLogger.debug("query-byte: con:" + condition.getColumn() + ", dir:" + dir.getDirName() + ", time:" + (System.currentTimeMillis() - s));
        return new QueryResult(total, dir).setData(result).setLineRangeList(lineRangeList);
    }

    private static QueryResult queryByGrep(byte[] bts, ColumnCondition condition, DataPack dir, boolean getData) throws Exception {
        List<DataLine> dataResult = null;
        List<LineRange> lineRangeList = null;
        if (getData) {
            dataResult = new ArrayList<DataLine>();
        } else {
            lineRangeList = new ArrayList<LineRange>();
        }
        long startNum = CommonUtil.parseInt(dir.getDirName()) * ServerConstants.PARK_SIZ;
        int dataIndex = 0;
        int total = 0;
        //TableInfo tableInfo = CacheUtil.readTableInfo(condition.getDataBase(), condition.getTable());
        //int lineNumLenth = tableInfo.getColumnDef(Constants.COLUMN_ID).getLength();
        byte[] search = condition.getSearchKey().getBytes(Constants.DEFAULT_CHARSET);
        List<Integer> idxList = BoyerMoore.match(search, bts);
        for (Integer idx : idxList) {
            int lineStart = 0;  //行开始索引
            int textStart = -1; //正文开始索引
            for (int i = idx; i >= 0; i--) {    //往前查找，
                if (bts[i] == Constants.STACK_SPLIT) {
                    textStart = i + 1;  //正文开始 = 分隔符号 + 1
                } else if (bts[i] == Constants.LINE_SEPARATOR) {
                    lineStart = i + 1;  //行开开始 = 行分割符号 + 1
                    break;
                }
            }
            if (textStart == -1) {  //如果没找到行开始标记，说明可能是grep 到了 行号，那么这个不算，跳过
                continue;
            }
            int lineNumLenth = textStart - 1 - lineStart;
            int lineNum = 0;
            if (ServerConstants.USE_64) {
                lineNum = (int) Convert10To64.unCompressNumberByLine(new String(bts, lineStart, lineNumLenth, Constants.DEFAULT_CHARSET));
            } else {
                lineNum = Integer.parseInt(new String(bts, lineStart, lineNumLenth, Constants.DEFAULT_CHARSET));
            }
            long id = startNum + lineNum;
            if (getData) {  //数据查询, 追加数据, 不过这个分支目前跑不到，因为拿数据都是根据ID去获取的
                if (condition.getLimit() > 0 && dataResult.size() < condition.getLimit()
                        && ++dataIndex > condition.getStart()) {
                    int to = 0;
                    for (int i = textStart; i < bts.length; i++) {  //往后查找
                        if (bts[i] == Constants.LINE_SEPARATOR) {
                            to = i;
                            break;
                        }
                    }
                    dataResult.add(new DataLine(new String(bts, textStart, to - textStart, Constants.DEFAULT_CHARSET), id));
                }
            } else { //count查询, 追加id
                int size = lineRangeList.size();
                if (size > 0) {
                    LineRange rangeLast = lineRangeList.get(size - 1);
                    long last = rangeLast.getEnd();
                    long dif = id - last;
                    if (dif == 0) {
                        continue;
                    } else if (dif == 1) {   //连续的
                        rangeLast.setEnd(id);
                    } else {
                        lineRangeList.add(new LineRange(id));
                    }
                } else {
                    lineRangeList.add(new LineRange(id));
                }
            }
            total++;

        }
        return new QueryResult(total, dir).setData(dataResult).setLineRangeList(lineRangeList);
    }


    /**
     * 数据查询(data)
     * @param bts
     * @param condition
     * @param dir
     * @return
     * @throws Exception
     */
    public static QueryResult search(byte[] bts, ColumnCondition condition, DataPack dir) throws Exception {
        if (condition.getLimit() == 0) {
            return new QueryResult(0, dir);
        }
        return query(bts, condition, dir, true);
    }

    /**
     * 数量查询(count)
     * @param bts
     * @param condition
     * @param dir
     * @return
     * @throws Exception
     */
    public static QueryResult count(byte[] bts, ColumnCondition condition, DataPack dir) throws Exception {
        /*String key = condition.toCountKey() + "=" + dir.getDirName();
        QueryResult resInCache = QueryCache.get(key);
        if (resInCache != null) {
            return resInCache;
        }*/ //缓存移到外层了，为了节省获取数据块数据的时间
        QueryResult res = query(bts, condition, dir, false);
        //QueryCache.put(key, res);
        return res;
    }

    /**
     * 根据ID查询
     * @param bytes
     * @param con
     * @param dir
     * @param ownCol
     * @return
     * @throws Exception
     */
    public static List<String> searchById(byte[] bytes, ColumnCondition con, DataPack dir, String ownCol) throws Exception {
        List<String> result = new ArrayList<String>();
        if (con.getLimit() == 0) {  //limit为0 就不用计算了
            return result;
        }
        if (bytes == null) {    //DP data 块未完全同步, 则全部置null
            for (int i = 0, k = con.getLimit(); i < k; i++) {
                result.add(null);
            }
            return result;
        }
        int jump = con.getStart();   //需要根据start跳过的数据
        TableInfo tableInfo = CacheUtil.readTableInfo(con.getDataBase(), con.getTable());
        ColumnDef def1 = tableInfo.getColumnDef(con.getColumn());
        ColumnDef def2 = tableInfo.getColumnDef(ownCol);
        DecimalFormat df = DataBaseUtil.getDataFormat(def1.getLength());
        //获取null 个数
        IndexInfo indexInfo = CacheUtil.getIndexInfo(con.getDataBase(), con.getTable(), dir.getDirName(), ownCol);
        int nullCount = indexInfo == null ? 0 : indexInfo.getNullCount();
        int length = 0;
        int idLength = def1.getLength();
        if (ServerConstants.USE_64) {
            length = LongLengthMapping.get64By10(def1.getLength()) + 2;
            if (def2.isLong() || def2.isEnum()) {
                length += LongLengthMapping.get64By10(def2.getLength());
            } else {
                length += def2.getLength();
            }
            idLength = LongLengthMapping.get64By10(idLength);
        } else {
            length = def1.getLength() + def2.getLength() + 2;   // /t /n 俩个占位符
        }
        boolean fixedStart = def1 != null && def1.getLength() > 0 && def2 != null && def2.getLength() > 0;
        int lastFind = 0; //上一次检索的位置
        //预估起始未知
        out:
        for (QueryRange range : con.getQueryRanges()) {
            int startIndex = 0;
            int startLinePre = (int) (range.getStart() % ServerConstants.PARK_SIZ);   //查询的起始行号
            int endLinePre = (int) (range.getEnd() % ServerConstants.PARK_SIZ);   //查询的结束行号
            byte[] minByte = null;
            byte[] maxByte = null;
            if (ServerConstants.USE_64) {
                minByte = DataBaseUtil.formatData((long) startLinePre, def1.getLength(), true).getBytes();
                maxByte = DataBaseUtil.formatData((long) endLinePre, def1.getLength(), true).getBytes();
            } else {
                minByte = df.format(startLinePre).getBytes();
                maxByte = df.format(endLinePre).getBytes();
            }
            /*if (lastFind == 0) {
                if (fixedStart) {  //计算位置
                    int startLine = (int) (startLinePre - (nullCount * startLinePre / ServerConstants.PARK_SIZ)); //根据nullCount估算起始行
                    startIndex = startLine * length;
                    if (startIndex > 0) {   //如果预估索引位大于0，就去查找真正的开始索引位
                        //查找真正的下标位置
                        startIndex = findStartIndex(bytes, startIndex, length, idLength, minByte);
                    } else {
                        startIndex = 0; //如果小于0 就强制更改为0吧
                    }
                } else {
                    startIndex = findStartIndex(bytes, minByte);
                }
            } else {
                startIndex = lastFind;
            }*/
            if (fixedStart) {  //计算位置
                int startLine = (int) (startLinePre - (nullCount * startLinePre / ServerConstants.PARK_SIZ)); //根据nullCount估算起始行
                startIndex = startLine * length;
                if (startIndex > 0) {   //如果预估索引位大于0，就去查找真正的开始索引位
                    //查找真正的下标位置
                    startIndex = findStartIndex(bytes, startIndex, length, idLength, minByte);
                } else {
                    startIndex = 0; //如果小于0 就强制更改为0吧
                }
            } else {
                startIndex = findStartIndex(bytes, minByte);
            }

            int lineNumStart = startIndex;
            int lineNumEnd = 0;
            int dataStart = 0;
            int commonSeg = 1;
            ColumnDef numDef = tableInfo.getColumnDef(Constants.COLUMN_ID);
            int lineSeg = numDef.getLength() + 1;
            ColumnDef def = tableInfo.getColumnDef(ownCol);
            int numSeg = def.getLength() + 1;
            if (ServerConstants.USE_64) {
                lineSeg = LongLengthMapping.get64By10(lineSeg - 1) + 1;
                if (def.isLong() || def.isEnum()) {
                    numSeg = LongLengthMapping.get64By10(numSeg - 1) + 1;
                }
            }
            for (int i = startIndex, k = bytes.length; i < k;) {
                if (bytes[i] == Constants.LINE_SEPARATOR) {
                    if (lineNumEnd == 0) {
                        logger.warn("lineNumEnd is 0, dir:" + dir.getDirName() + ", ownCol:" + ownCol + ", ColumnCondition:" + con);
                        i += lineSeg;
                        continue;
                    }
                    if (lineNumStart > lineNumEnd) {
                        logger.warn(lineNumStart + ">" + lineNumEnd);
                    }
                    byte[] lineNum = Arrays.copyOfRange(bytes, lineNumStart, lineNumEnd);
                    //重置，只允许为0时，进行赋值，所以临时保存当前数值
                    int lineNumEndNow = lineNumEnd;
                    lineNumEnd = 0;
                    /*if ("entryURL".equals(ownCol)) {
                        System.out.println("entryURL:line:" + new String(lineNum));
                    }*/
                    if (DataBaseUtil.compare(lineNum, minByte) < 0) {
                        lineNumStart = i + 1;   //重置起始索引，指向下一行第一个元素
                        i += lineSeg;
                        continue;
                    }
                    if (DataBaseUtil.compare(lineNum, maxByte) > 0) {  //如果当前行号大于查询的max，直接结束
                        break;
                    }
                    //记录数据
                    Long line = null;
                    if (ServerConstants.USE_64) {
                        line = Convert10To64.unCompressNumberByLine(new String(bytes, lineNumStart, lineNumEndNow - lineNumStart));
                    } else {
                        line = CommonUtil.parseLong2(new String(bytes, lineNumStart, lineNumEndNow - lineNumStart));
                    }
                    lastFind = i + lineSeg;
                    if (line == null) {
                        lineNumStart = i + 1;   //重置起始索引，指向下一行第一个元素
                        i += lineSeg;
                        continue;
                    }
                    while (startLinePre < line) {   //之前的空白位置，置null
                        startLinePre++; //标明此索引行号已经拿到数据了
                        if (--jump < 0) {
                            result.add(null);
                            if (result.size() >= con.getLimit()) {
                                break out;
                            }
                        }
                    }
                    startLinePre++; //标明此索引行号已经拿到数据了
                    if (--jump < 0) {
                        result.add(new String(bytes, dataStart, i - dataStart, Constants.DEFAULT_CHARSET));
                        if (result.size() >= con.getLimit()) {
                            break out;
                        }
                    }
                    lineNumStart = i + 1;   //重置起始索引，指向下一行第一个元素
                    i += lineSeg;
                } else if (bytes[i] == Constants.STACK_SPLIT && lineNumEnd == 0) {
                    lineNumEnd = i;
                    dataStart = i + 1;
                    i += numSeg;
                } else {
                    i += commonSeg;
                }
            }
            //对于没有查到的行号补null
            while (startLinePre <= endLinePre) {
                startLinePre++;
                if (--jump < 0) {
                    result.add(null);
                    if (result.size() >= con.getLimit()) {
                        break out;
                    }
                }
            }
        }
        return result;
    }

    /**
     * 非定长查找起始位置
     * @param bytes
     * @param minByte
     * @return
     */
    private static int findStartIndex(byte[] bytes, byte[] minByte) {
        if (bytes.length == 0) {
            return 0;
        }
        int spCount = 100;
        int seg = Math.max(bytes.length / spCount, 1);    //拆分成1000个段
        int lineStart = -1;
        int lineEnd = -1;
        for (int ii = 1; ii < spCount; ii++) {
            int index = bytes.length - seg * ii;
            for (int i = index; i >= 0; i--) {
                if (lineEnd == -1) { //先找end
                    if (bytes[i] == Constants.STACK_SPLIT) {
                        lineEnd = i;
                    }
                } else {    //找到end再找start
                    if (bytes[i] == Constants.LINE_SEPARATOR) {
                        lineStart = i + 1;
                        break;
                    }
                }
            }
            if (lineStart > -1 && lineEnd > -1) {
                byte[] bs = Arrays.copyOfRange(bytes, lineStart, lineEnd);
                int res = DataBaseUtil.compare(bs, minByte);
                if (res <= 0) {
                    return lineStart;  //找到的数据比 minByte 小或等于，就从此位置开始查找
                } else {    //重找
                    lineStart = -1;
                    lineEnd = -1;
                }
            } else {
                return 0;
            }
        }
        return 0;
    }

    /**
     * 找到开始的数组下标，比 minByte 小就行
     * @param bytes
     * @param startIndex
     * @param length
     * @param idLength
     * @param minByte
     * @return
     */
    public static int findStartIndex(byte[] bytes, int startIndex, int length, int idLength, byte[] minByte) {
        int next = startIndex - length * 100; //往前找的 下十个
        int end = startIndex + idLength;//结束下标
        if (end > bytes.length) {
            /*next = bytes.length - (bytes.length % length) - length;   //跳跃从最后查找
            return findStartIndex(bytes, next, length, idLength, minByte);*/
            return 0;   //如果跳过头了，说明肯定是哪里有问题。最后发现，某列全空，全部为NULL时，可能导致 bytes.length = 0，next = 负数
        }
        byte[] bs = Arrays.copyOfRange(bytes, startIndex, end);
        int res = DataBaseUtil.compare(bs, minByte);
        if (res <= 0) {
            return startIndex;  //找到的数据比 minByte 小或等于，就从此位置开始查找
        } else if (next >= 0) { //递归往前找
            return findStartIndex(bytes, next, length, idLength, minByte);
        } else {
            return 0;
        }
    }
}
