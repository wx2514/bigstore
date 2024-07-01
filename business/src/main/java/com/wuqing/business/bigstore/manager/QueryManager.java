package com.wuqing.business.bigstore.manager;

import com.alibaba.fastjson.JSON;
import com.wuqing.business.bigstore.bean.Holder;
import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.bean.SpaceInfo;
import com.wuqing.business.bigstore.bean.TableInfo;
import com.wuqing.business.bigstore.cache.BaseCache;
import com.wuqing.business.bigstore.cache.QueryCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.business.bigstore.exception.BusinessException;
import com.wuqing.business.bigstore.exception.ThrowBusinessException;
import com.wuqing.business.bigstore.lucene.LuceneResult;
import com.wuqing.business.bigstore.lucene.LuceneUtil;
import com.wuqing.business.bigstore.util.*;
import com.wuqing.business.bigstore.util.filterjudge.*;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.HessianUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;

public class QueryManager {

    private final static Logger queryTimeLogger = LoggerFactory.getLogger("query-time-log");

    private final static Logger slowQueryLogger = LoggerFactory.getLogger("slow-query-time");

    private static Set<String> queryCacheColumns = Params.getQueryCacheColumnsSet();

    /**
     * 查询，总入口
     * @param condOrg
     * @throws Exception
     */
    public static DataResult query(final Condition condOrg) throws Exception {
        //深拷贝，避免此对象在序列化的时候，被变更。 之前遇到过在序列化的时候，对象发生了改变，会卡住。导致超时，涨见识了。
        Condition condn = (Condition) HessianUtil.unserialize(HessianUtil.serialize(condOrg));
        if (queryTimeLogger.isDebugEnabled()) {
            queryTimeLogger.debug(condn.toString());
        }
        if (!check(condn)) {
            throw new BusinessException("condition is invalid, condn: " + JSON.toJSONString(condn));
        }
        if (!CommonUtil.isEmpty(condn.getId())) {
            return queryById(condn);
        }
        DataResult result = new DataResult();
        long st = System.currentTimeMillis();
        long s = st;
        List<ColumnCondition> conditionList = condn.toColumnConditions();
        //获取相关DP关系 (可疑DP + 强相关DP)
        List<DataPack> packRelationAll = null;
        TableInfo tableInfo = CacheUtil.readTableInfo(condn.getDataBase(), condn.getTable());
        //追加列信息
        ColumnDef[] fieldColumns = addColumnInfo(result, tableInfo, condn);

        for (ColumnCondition con : conditionList) {
            ColumnDef columnDef = tableInfo.getColumnDef(con.getColumn());
            if (columnDef == null) {
                throw new BusinessException("unknow column: " + con.getColumn());
            }
            if (con.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL
                    && !columnDef.isReverseIndex()) {   //如果查询类型是全文检索 && 当前列不是全文检索列，抛异常
                throw new BusinessException("column don't support full text retrieval: " + con.getColumn());
            }
            if (columnDef.isEnum()) {   //如果是枚举类型, 将检索条件变更为枚举
                EnumInfo enumInfo = CacheUtil.getEnumInfo(condn.getDataBase(), condn.getTable(), con.getColumn());
                if (con.getSearchKey() != null) {
                    Long search = enumInfo.getIndex(con.getSearchKey());
                    if (search == null) {
                        search = -1L;
                    }
                    con.setSearch(search);
                }
                List<String> searchList = con.getSearchList();
                if (searchList != null) {
                    for (int i = 0, k = searchList.size(); i < k; i++) {
                        String v = searchList.get(i);
                        Long search = enumInfo.getIndex(v);
                        if (search == null) {
                            search = -1L;
                        }
                        //把查询条件转化成枚举索引
                        searchList.set(i, String.valueOf(search));
                    }
                }
            }
            long ss = System.currentTimeMillis();
            List<DataPack> packRelEach = findPackRelation(con, packRelationAll);
            queryTimeLogger.debug("[" + con.getColumn() + "] " + "find-dp-time:" + (System.currentTimeMillis() - ss) + ", dp-size:" + packRelEach.size());
            if (packRelationAll == null) {
                packRelationAll = packRelEach;
            } else {    //取交集
                packRelationAll = intersection(packRelationAll, packRelEach);
            }
        }
        queryTimeLogger.debug("find-dp-all-time:" + (System.currentTimeMillis() - s));
        int limitHolder = condn.getLimit();
        int startHolder = condn.getStart();
        List<List<DataPack>> packRelationList = split(packRelationAll, condn);
        queryTimeLogger.debug("pack-relation-size:" + packRelationList.size());
        for (List<DataPack> packRelation : packRelationList) {
            int limitReplace = (int) (limitHolder - result.getDatas().size());
            if (limitReplace <= 0) {
                break;
            }
            int startReplace = (int) (startHolder - result.getTotal());
            if (startReplace < 0) {
                startReplace = 0;
            }
            condn.setLimit(limitReplace);
            condn.setStart(startReplace);
            ColumnCondition idQuery = null;
            s = System.currentTimeMillis();
            if (CommonUtil.isEmpty(packRelation)) {
                return result;  //没有找到相关数据块，则直接返回结果吧
            }
            List<Future<List<QueryResult>>> futureList = new ArrayList<Future<List<QueryResult>>>();
            //List<ColumnCondition> conByReverse = new ArrayList<ColumnCondition>();
            for (int i = 0, k = conditionList.size(); i < k; i++) {
                final ColumnCondition con = conditionList.get(i);
                if (Constants.COLUMN_ID.equals(con.getColumn())) {
                    idQuery = con;
                    continue;
                }
                con.setDataDirLsit(packRelation);
                Future<List<QueryResult>> f = PoolUtil.QUERY_FIX_POLL_QUERY.submit(new Callable<List<QueryResult>>() {
                    @Override
                    public List<QueryResult> call() throws Exception {
                        //long ss = System.currentTimeMillis();
                        List<QueryResult> results = countData(con);
                        //queryTimeLogger.debug("find-count-con:" + con.getColumn() + ":" + (System.currentTimeMillis() - ss));
                        return results;
                    }
                });
                futureList.add(f);
                //}
            }
            List<QueryResult> queryResList = null;
            for (Future<List<QueryResult>> f : futureList) {
                List<QueryResult> qrList = f.get();
                if (qrList == null) {
                    continue;
                }
                if (queryResList == null) {   //如果是第一次，或 是全文检索并且又是最后一个条件
                    queryResList = qrList;
                } else {    //合并数据
                    //因为dirList一样，所以结果的数组长度肯定也一样(当然可能结果的total=0)
                    //long ss = System.currentTimeMillis();
                    queryResList = intersectionResult(queryResList, qrList);
                    //queryTimeLogger.debug("find-merge-con:" + (System.currentTimeMillis() - ss));
                }
            }
            //根据id条件再做一次过滤
            filterByIdCondition(idQuery, queryResList);
            long countTime = System.currentTimeMillis() - s;
            s = System.currentTimeMillis();
            int start = condn.getStart();
            int limit = condn.getLimit();
            long total = 0L;
            //计算查询condition
            List<ColumnCondition> conList = new ArrayList<ColumnCondition>();
            if (condn.getLimit() == 0) {    //如果limit=0 说明可能只是拿count, 那么直接获取数量就好了
                for (QueryResult qs : queryResList) {
                    total += qs.getTotal();     //统计总数
                }
                result.addTotal(total);
            } else {    //data查询
                for (QueryResult qs : queryResList) {
                    if (qs.getTotal() == 0 || qs.getLineRangeList() == null) {
                        continue;
                    }
                    ColumnCondition con = new ColumnCondition(condn.getDataBase()).setTable(condn.getTable()).setColumn(Constants.COLUMN_ID)
                            .setStart(condn.getStart()).setLimit(condn.getLimit()).setDataDirLsit(CommonUtil.asList(qs.getDir())).setType(Constants.QUERY_TYPE_RANGE);
                    for (LineRange r : qs.getLineRangeList()) {
                        con.addQueryRange(new QueryRange(r.getStart(), r.getEnd()));
                    }
                    long count = qs.getTotal();
                    total += count;     //统计总数
                    if (count == 0) {
                        con.setStart(0);
                        con.setLimit(0);
                    } else if (start > count) {
                        con.setStart(0);
                        con.setLimit(0);
                        start -= count;
                    } else {
                        con.setStart(start);
                        int thisLimit = (int) Math.min((count - start), limit);
                        con.setLimit(thisLimit);
                        start = 0;
                        limit -= thisLimit;
                    }
                    conList.add(con);
                }
                //queryTimeLogger.debug("create condtions:" + (System.currentTimeMillis() - s));
                //s = System.currentTimeMillis();
                result.addTotal(total);
                if (condn.getOneMachineAggregationLimit() > 0 && total > condn.getOneMachineAggregationLimit()) {
                    throw new ThrowBusinessException("[ThrowBusinessException] total[" + total + "] is more than limit[" + condn.getOneMachineAggregationLimit() + "]");
                }
                int type = getAggregation(condn.getDataBase(), condn.getTable(), condn.getFields());
                if (type > 0 || condn.getGroupBy() != null) {
                    result.setDatasByAggregation(getAggregationDataById(condn.getDataBase(), condn.getTable(), condn.getFields(), conList, condn.getGroupBy()));
                    result.setDatas(resultMap2List(result.getDatasByAggregation()));
                } else {
                    result.addDatas(getDataById(condn.getDataBase(), condn.getTable(), condn.getFields(), conList, condn.getGroupBy()));
                }
            }
            queryTimeLogger.debug("find-count-time:" + countTime + ", find-data-time:" + (System.currentTimeMillis() - s));
            filter(result, condn); //过滤掉查询条件中的查询
            doWithDecimal(result, fieldColumns);
            doWithSpecialFeild(result, condn.getFieldAsSpe());
        }

        long timeRt = System.currentTimeMillis() - st;
        queryTimeLogger.debug("find-all-time:" + timeRt);
        if (timeRt > 1000) {
            slowQueryLogger.debug("time:" + timeRt + "\n" + condn.toString());
        }
        //这里先不处理，放到汇总阶节点去处理
        //doWithOtherFunction(result, condn);
        return result;
    }

    private static List<List<DataPack>> split(List<DataPack> packRelationAll, Condition condn) throws Exception {
        int typeA = getAggregation(condn.getDataBase(), condn.getTable(), condn.getFields());
        List<List<DataPack>> list = new ArrayList<>();
        if (!condn.isIgnoreCount() || typeA > 0 || condn.getGroupBy() != null) {
            //如果不忽略count计算 或 有聚合函数 或 有分组
            list.add(packRelationAll);
        } else {
            int i = 0;
            List<DataPack> each = new ArrayList<>();
            for (DataPack dp : packRelationAll) {
                 each.add(dp);
                 i++;
                 if (i >= 100) {
                     list.add(each);
                     each = new ArrayList<>();
                     i = 0;
                 }
            }
            if (each.size() > 0) {
                list.add(each);
            }
        }
        return list;
    }

    /**
     * 处理嵌套函数
     * 只在TCP-Server模式下，最终聚合时被调用【本地接口测试可能调用不到这个方法，请注意】
     * @param result
     * @param condn
     * @throws Exception
     */
    public static void doWithOtherFunction(DataResult result, Condition condn) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(condn.getDataBase(), condn.getTable());
        ColumnDef[] cols = tableInfo.getColumnDefs();
        cols = getSelColumn(cols, condn.getFields());
        //最多支持10层嵌套函数
        for (int j = 0; j < 10; j++) {
            doWithOtherFunctionSub(result, cols);
            //结束循环
            boolean over = true;
            for (int i = 0, k = cols.length; i < k; i++) {
                List<Integer> otherAggr = cols[i].getOtherAggregation();
                if (CommonUtil.isEmpty(otherAggr)) {
                    continue;
                }
                //如果找到还有函数没处理的，继续执行 doWithOtherFunctionSub
                over = false;
                break;
            }
            //结束
            if (over) {
                break;
            }
        }
    }

    private static void doWithOtherFunctionSub(DataResult result, ColumnDef[] cols) {
        //根据位置需要聚合的函数
        Map<Integer, FunctionData> funcMap = new HashMap<>();
        for (int i = 0, k = cols.length; i < k; i++) {
            List<Integer> otherAggr = cols[i].getOtherAggregation();
            if (CommonUtil.isEmpty(otherAggr)) {
                continue;
            }
            funcMap.put(i + 1, new FunctionData(otherAggr.remove(0)));
        }
        //说明没有嵌套函数了，结束
        if (funcMap.isEmpty()) {
            return;
        }
        Map<Integer, List<String>> colData = new ConcurrentSkipListMap<>();
        //行转列
        for (List<String> line : result.getDatas()) {
            for (int i = 0, k = line.size(); i < k; i++) {
                List<String> col = colData.get(i);
                if (col == null) {
                    col = new ArrayList<>();
                    colData.put(i, col);
                }
                String s = line.get(i);
                col.add(s);
            }
        }
        for (Map.Entry<Integer, List<String>> entry : colData.entrySet()) {
            FunctionData func = funcMap.get(entry.getKey());
            List<String> colList = entry.getValue();
            if (func == null || colList == null) {
                continue;
            }
            for (String col : colList) {
                func.addData(col);
            }
            //数据替换
            colList.clear();
            colList.addAll(func.getDataByAggregation());
        }
        int sizeMax = 0;
        for (Map.Entry<Integer, List<String>> entry : colData.entrySet()) {
            //第一列是预留列【id】
            if (entry.getKey() == 0) {
                continue;
            }
            sizeMax = Math.max(sizeMax, entry.getValue().size());
        }
        //列转行
        List<List<String>> dataList = new ArrayList<>();
        for (int i = 0; i < sizeMax; i++) {
            List<String> line = new ArrayList<>();
            for (Map.Entry<Integer, List<String>> entry : colData.entrySet()) {
                List<String> colList = entry.getValue();
                if (colList.size() > i) {
                    line.add(colList.get(i));
                } else {
                    line.add(null);
                }
            }
            dataList.add(line);
        }
        result.setDatas(dataList);
    }

    private static void doWithSpecialFeild(DataResult result, List<FieldAs> fieldAsSpe) throws BusinessException {
        if (CommonUtil.isEmpty(fieldAsSpe)) {
            return;
        }
        for (List<String> list : result.getDatas()) {
            String idValue = list.get(0);

            for (int i = fieldAsSpe.size() - 1; i >= 0; i--) {
                FieldAs as = fieldAsSpe.get(i);
                if (SpecialFieldEnum.ID.name().equals(as.getName().toUpperCase())) {
                    list.add(1, idValue);
                } else if (SpecialFieldEnum.WINDOW_START.name().equals(as.getName().toUpperCase())) {
                    String winStart = idValue.split("=")[0];
                    list.add(1, winStart);
                } else {
                    throw new BusinessException("special feild is invalid");
                }
            }
        }
        //处理column头
        for (int i = fieldAsSpe.size() - 1; i >= 0; i--) {
            FieldAs fs = fieldAsSpe.get(i);
            String as = fs.getAs();
            if (CommonUtil.isEmpty(as)) {
                as = fs.getName();
            }
            result.getColumns().add(1, as);
        }
    }

    /**
     * 追加列信息
     * @param result
     * @param tableInfo
     * @param condn
     * @throws BusinessException
     */
    private static ColumnDef[] addColumnInfo(DataResult result, TableInfo tableInfo, Condition condn) throws BusinessException {
        //获取查询的列名
        ColumnDef[] columnDefs = getSelColumn(tableInfo.getColumnDefs(), condn.getFields());
        if (columnDefs != null) {
            boolean eq = columnDefs.length == condn.getFields().size();
            eq = eq && (condn.getFields().size() == condn.getFieldAs().size());
            List<String> list = new ArrayList<String>();
            for (int i = 0, k = columnDefs.length; i < k; i++) {
                ColumnDef col = columnDefs[i];
                if (eq && condn.getFieldAs().get(i) != null) {
                    list.add(condn.getFieldAs().get(i));
                } else {
                    list.add(col.getName());
                }
            }
            result.setColumns(list);
        }
        return columnDefs;
    }

    private static void doWithDecimal(DataResult result, ColumnDef[] fieldColumns) {
        if (result == null || fieldColumns == null) {
            return;
        }
        List<Integer> decimalColList = new ArrayList<>();
        int idx = 0;
        for (ColumnDef def : fieldColumns) {
            if (def.isDecimal()) {
                decimalColList.add(idx);
            }
            idx++;
        }
        if (decimalColList.isEmpty()) {
            return; //如果没有小数，直接返回结束
        }
        for (List<String> lines : result.getDatas()) {
            for (Integer i : decimalColList) {
                int dataIdx = i + 1;   //数据列第0列是预留的, 所以数据idx要加1
                String l = lines.get(dataIdx);
                if (CommonUtil.isEmpty(l)) {
                    continue;
                }
                long lv = Long.parseLong(l);
                //如果不是count
                if (!ColumnDef.isCount(fieldColumns[i].getAggregation())
                        && !ColumnDef.isCountDistinct(fieldColumns[i].getAggregation())) {
                    int length = fieldColumns[i].getDecimalLength();
                    BigDecimal d = BigDecimal.valueOf(lv).divide(BigDecimal.valueOf(CommonUtil.pow10(length)));
                    d.setScale(length);
                    l = d.toString();
                }
                lines.set(dataIdx, l);
            }
        }

    }

    /**
     * 根据Id查询条件做一次过滤
     * @param idQuery
     * @param queryResList
     */
    private static void filterByIdCondition(ColumnCondition idQuery, List<QueryResult> queryResList) {
        if (idQuery == null || CommonUtil.isEmpty(queryResList)) {  //有些不需要过滤的直接返回
            return;
        }
        QueryRange idQueryRange = null;
        if (idQuery.getType() == Constants.QUERY_TYPE_EQUAL) {
            long id = -1;
            if (CommonUtil.isEmpty(idQuery.getSearchKey())) {
                id = idQuery.getSearch();
            } else {
                id = CommonUtil.parseLong(idQuery.getSearchKey(), -1);
            }
            idQueryRange = new QueryRange(id, id);
        } else if (idQuery.getType() == Constants.QUERY_TYPE_RANGE) {
            idQueryRange = idQuery.getQueryRanges().get(0);
        }
        for (Iterator<QueryResult> qsit = queryResList.iterator(); qsit.hasNext();) {
            QueryResult qs = qsit.next();
            for (Iterator<LineRange> it = qs.getLineRangeList().iterator(); it.hasNext();) {
                LineRange lr = it.next();
                if (lr.getEnd() < idQueryRange.getStart()) {
                    qs.setTotal(qs.getTotal() - (lr.getEnd() - lr.getStart() + 1));
                    it.remove();
                    continue;
                }
                if (lr.getStart() > idQueryRange.getEnd()) {
                    qs.setTotal(qs.getTotal() - (lr.getEnd() - lr.getStart() + 1));
                    it.remove();
                    continue;
                }
                long startMax = Math.max(lr.getStart(), idQueryRange.getStart());
                long endMin = Math.min(lr.getEnd(), idQueryRange.getEnd());
                long cha = (lr.getEnd() - lr.getStart()) - (endMin - startMax);
                lr.setStart(startMax);
                lr.setEnd(endMin);
                qs.setTotal(qs.getTotal() - cha);
            }
            if (qs.getTotal() == 0) {
                qsit.remove();
            } else if (qs.getTotal() < 0) {
                queryTimeLogger.warn("queryResult total is :" + qs.getTotal());
            }
        }

    }

    private static void filter(DataResult result, Condition con) {
        if (CommonUtil.isEmpty(con.getConditionSubFilter())) {
            return;
        }
        for (ConditionSub condition : con.getConditionSubFilter()) {
            int idx = -1;
            for (int i = 0, k = result.getColumns().size(); i < k; i++) {
                if (result.getColumns().get(i).equals(condition.getColumn())) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                continue;   //没找到就跳过
            }
            String search = condition.getSearchKey();
            if (search == null) {
                search = String.valueOf(condition.getSearch());
            }
            FilterJudge filterJudge = null;
            if (condition.getType() == Constants.QUERY_TYPE_RANGE) {
                filterJudge = new FilterRangeJudge(condition.getQueryRanges());
            } else if (condition.getType() == Constants.QUERY_TYPE_LIKE) { //模糊匹配
                filterJudge = new FilterLikeJudge(condition.getSearchKey());
            } else if (condition.getType() == Constants.QUERY_TYPE_KEY_LIKE_AND) {
                filterJudge = new FilterAndLikeJudge(condition.getSearchList());
            } else if (condition.getType() == Constants.QUERY_TYPE_KEY_LIKE_OR) {
                filterJudge = new FilterOrLikeJudge(condition.getSearchList());
            } else if (condition.getType() == Constants.QUERY_TYPE_NOT_EQUAL) {
                filterJudge = new FilterNotEqualJudge(search);
            } else if (condition.getType() == Constants.QUERY_TYPE_NOT_LIKE) {
                filterJudge = new FilterNotLikeJudge(search);
            } else if (condition.getType() == Constants.QUERY_TYPE_IN) {
                String[] searchs = new String[condition.getSearchList().size()];
                int k = 0;
                for (String s : condition.getSearchList()) {
                    searchs[k++] = s;
                }
                filterJudge = new FilterInJudge(searchs);
            } else {
                filterJudge = new FilterEqualJudge(search);
            }
            List<List<String>> datas = result.getDatas();
            long subduction = 0;
            for (int i = 0, k = datas.size(); i < k; i++) {
                String dd = result.getDatas().get(i).get(idx);
                if (!filterJudge.isMath(dd)) {
                    datas.remove(i);
                    i--;
                    k--;
                    subduction++;
                }
            }
            result.setTotal(result.getTotal() - subduction);

        }
        //result.getColumns()
    }

    private static boolean check(Condition condn) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(condn.getDataBase(), condn.getTable());
        if (tableInfo == null) {
            throw new BusinessException("unknow table: " + condn.getTable());
        }
        if (condn.getGroupBy() != null) {
            String[] gps = condn.getGroupBy().replaceAll("\\s", "").split(",");
            List<String> gpsList = new ArrayList<>();
            for (String gp : gps) {
                //gp = gp.trim();
                gpsList.add(gp);
                ColumnDef groupCol = tableInfo.getColumnDef(gp);
                if (groupCol == null) {
                    throw new BusinessException("group by column is invalid, column:" + condn.getGroupBy());
                }
            }
            ColumnDef[] columnDefs = getSelColumn(tableInfo.getColumnDefs(), condn.getFields());
            for (ColumnDef col : columnDefs) {
                //普通字段查询 并且 查询的字段 不在group by中
                if (col.getAggregation() == 0 && !gpsList.contains(col.getName())) {
                    throw new BusinessException("select filed is not in group by field, column:" + col.getName());
                }
            }
        }
        boolean empty = true;   // 查询条件为空
        int idCount = 0;
        int orderColumnIdx = -1;
        List<ConditionSub> filter = new ArrayList<>();
        int k = condn.getConditionSubList().size();
        for (int i = 0; i < k; i++) {
            ConditionSub sub = condn.getConditionSubList().get(i);
            ColumnDef def = tableInfo.getColumnDef(sub.getColumn());
            if (def == null) {
                int idx = -1;
                int ii = 0;
                for (String as : condn.getFieldAs()) {
                    if ((as != null) && as.equals(sub.getColumn())) {
                        idx = ii;
                        break;
                    }
                    ii++;
                }
                if (idx > -1) { //as里面是否包括条件中的字段，如果是则可以继续
                    String realColumn = condn.getFields().get(idx);
                    ColumnDef realColumnDef = tableInfo.getColumnDef(realColumn);
                    if (realColumnDef == null) {    //如果找不到则放到filter里
                        filter.add(sub);
                        continue;
                    } else {    //否则修正查询列
                        def = realColumnDef;
                        sub.setColumn(realColumn);
                    }
                } else {
                    throw new BusinessException("column is invalid, col:" + sub.getColumn());
                }
            }
            if (def.isOrderSpace() && orderColumnIdx == -1) {
                orderColumnIdx = i;
            }
            if (k == 1 && Constants.COLUMN_ID.equals(sub.getColumn())
                    && sub.getType() == Constants.QUERY_TYPE_EQUAL) {   //如果是ID查询，并且只有这一个条件，则特殊处理
                if (CommonUtil.isEmpty(sub.getSearchKey())) {
                    condn.addId(sub.getSearch());
                } else {
                    condn.addId(CommonUtil.parseLong(sub.getSearchKey(), -1));
                }
                idCount++;  //为了重新计算limit
            } else if (sub.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL) {
                String search = sub.getSearchKey();
                if (CommonUtil.isEmpty(search)) {
                    throw new BusinessException("searchKey is empty when full text search");
                }
                for (char searchChar : search.toCharArray()) {
                    for (char spChar : SplitBuilder.SPLIT_KEY_CHARS) {
                        if (searchChar == spChar) {
                            throw new BusinessException("searchKey is invalid when full text search");
                        }
                    }
                }
            } else if (sub.getType() == Constants.QUERY_TYPE_GREP) {
                if (CommonUtil.isEmpty(sub.getSearchKey())) {
                    throw new BusinessException("searchKey is empty when grep text");
                }
            } else if (sub.getType() == Constants.QUERY_TYPE_KEY_LIKE_AND
                    || sub.getType() == Constants.QUERY_TYPE_KEY_LIKE_OR) {
                if (CommonUtil.isEmpty(sub.getSearchList())) {
                    throw new BusinessException("searchList is empty when search by and or");
                }
                for (Object key : sub.getSearchList()) {
                    if (key == null) {
                        throw new BusinessException("search key is null when search by and or");
                    }
                }
            } else if (sub.getType() == Constants.QUERY_TYPE_EQUAL) {   //等于的时候根据字段类型做判定
                if (def.isLong() && sub.getSearch() == 0 && sub.getSearchKey() != null) {
                    Long search = CommonUtil.parseLong2(sub.getSearchKey());
                    if (search == null) {
                        throw new BusinessException("search key must be number, search:" + search);
                    }
                    sub.setSearch(search);
                }
            }

            if (!Constants.COLUMN_ID.equals(sub.getColumn())) {
                empty = false;  //如果不是filter过滤 也不是ID查询，则判定查询条件不为空
            }
        }

        if (orderColumnIdx > 0) {   //0的时候就是放在第一位的不需要处理
            //优化执行顺序
            ConditionSub sub = condn.getConditionSubList().get(orderColumnIdx);
            condn.getConditionSubList().remove(orderColumnIdx);
            condn.getConditionSubList().add(0, sub);
        } else if (orderColumnIdx == -1) {
            queryTimeLogger.warn("Space Condition is empty, con:" + JSON.toJSONString(condn));
        }

        if (idCount > 0) {  // 重新计算limit
            condn.setStart(0);
            condn.setLimit(idCount);
        }

        if (empty) {
            ColumnDef sel = null;
            for (ColumnDef def : tableInfo.getColumnDefs()) {
                if (def.isOrder()) {
                    sel = def;
                    break;
                }
            }
            if (sel != null) {
                condn.getConditionSubList().add(0, new ConditionSub().setColumn(sel.getName()).addQueryRange(new QueryRange(Long.MIN_VALUE, Long.MAX_VALUE)));
            }
        }

        for (ConditionSub ft : filter) {
            condn.getConditionSubList().remove(ft);
            condn.addConditionSubFilter(ft);
        }
        return true;    //检查成功
    }

    /**
     * 根据ID查找数据
     * @param condn
     * @return
     * @throws Exception
     */
    private static DataResult queryById(Condition condn) throws Exception {
        long s = System.currentTimeMillis();
        DataResult result = new DataResult();

        TableInfo tableInfo = CacheUtil.readTableInfo(condn.getDataBase(), condn.getTable());
        ColumnDef[] columnDefs = getSelColumn(tableInfo.getColumnDefs(), condn.getFields());
        if (columnDefs != null) {
            List<String> list = new ArrayList<String>();
            for (ColumnDef col : columnDefs) {
                list.add(col.getName());
            }
            result.setColumns(list);
        }

        List<ColumnCondition> conList = new ArrayList<ColumnCondition>();
        Map<String, List<Long>> group = new HashMap<String, List<Long>>();
        DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
        for (Long id : condn.getId()) {
            long dirNum = id / ServerConstants.PARK_SIZ;
            String dir = df.format(dirNum);
            List<Long> list = group.get(dir);
            if (list == null) {
                list = new ArrayList<Long>();
                group.put(dir, list);
            }
            list.add(id);
        }
        for (Map.Entry<String, List<Long>> entry : group.entrySet()) {
            ColumnCondition con = new ColumnCondition(condn.getDataBase()).setTable(condn.getTable()).setColumn(Constants.COLUMN_ID)
                    .setStart(condn.getStart()).setLimit(condn.getLimit()).setDataDirLsit(CommonUtil.asList(new DataPack(entry.getKey()))).setType(Constants.QUERY_TYPE_RANGE);
            for (Long id : entry.getValue()) {
                con.addQueryRange(new QueryRange(id, id));
            }
            conList.add(con);
        }

        result.setDatas(getDataById(condn.getDataBase(), condn.getTable(), condn.getFields(), conList, condn.getGroupBy()));
        result.setTotal(result.getDatas().size());
        queryTimeLogger.debug("find by id:" + (System.currentTimeMillis() - s));
        return result;
    }

    /**
     * 根据倒排索引查询
     * @param con   //查询条件
     * @param queryResList  //上一次查询的结果集
     * @return
     */
    private static List<QueryResult> countDataByReverse(ColumnCondition con, List<QueryResult> queryResList, boolean isLast) {
        List<QueryResult> result = new ArrayList<QueryResult>();
        //初始化一下
        for (QueryResult qr : queryResList) {
            QueryResult r = new QueryResult();
            r.setDir(qr.getDir());
            r.setLineRangeList(new ArrayList<LineRange>());
            result.add(r);
        }
        List<DataPack> dataDirList = con.getDataDirLsit();
        String rowFile = Params.getBaseDir() + con.getDataBase() + "/" + con.getTable() + "/rows/";
        Set<String> indexSet = new HashSet<String>();
        List<String> indexList = new ArrayList<String>();
        for (DataPack dir : dataDirList) {
            String dirName = dir.getDirName();
            String space = getSpaceDir(dirName);
            String index = rowFile + space + "/" + con.getColumn() + FileConfig.INDEX_FILE_SUFFIX;
            if (indexSet.add(index)) {
                indexList.add(index);
            }
        }
        List<LineRange> lineAll = new ArrayList<LineRange>();
        for (QueryResult qr : queryResList) {
            if (qr.getLineRangeList() != null) {
                lineAll.addAll(qr.getLineRangeList());
            }
        }
        int start = 0;
        int limit = (int) ServerConstants.PARK_SIZ * Constants.SPACE_SIZ;
        if (isLast) {
            start = con.getStart();
            limit = con.getLimit();
        }
        for (String index : indexList) {    //从分区倒排索引中查询
            LuceneResult luceneResult = LuceneUtil.search(index, start, limit, lineAll, con.getSearchKey());
            List<Long> numbers = luceneResult.getNumberList();
            long remainTotal = luceneResult.getTotal(); //还剩余的total, 最后会追加到最后一个模块
            //查询完毕后，再拆分到每个block块中去
            Collections.sort(numbers);
            int size = result.size();
            int ii = 0;  //被处理过的数据直接跳过，这个记录当前处理过的数据下标
            for (int i = 0, k = size; i < k; i++) {
                QueryResult qr = result.get(i);
                long startInBlock = Long.parseLong(qr.getDir().getDirName()) * ServerConstants.PARK_SIZ;
                long endInBlock = startInBlock + ServerConstants.PARK_SIZ - 1;
                for (int kk = numbers.size(); ii < kk; ) {
                    long v = numbers.get(ii);
                    if (v < startInBlock) {
                        ii++;
                        continue;   //获取下一个数值
                    }
                    if (v > endInBlock) {
                        break;  //跳出匹配下一个块
                    }
                    qr.addLineAtLast(v);
                    remainTotal--;
                    ii++;   //当前数据已经被处理，下标跳转到下一个
                }
            }
            if (remainTotal > 0 && size > 0) {
                QueryResult last = result.get(size - 1);
                last.setTotal(last.getTotal() + remainTotal); //虽然没有记录，但是为了保证total有效，仍然记录，放到最后一个pack
            }
        }
        return result;
    }

    /**
     * 根据ID 获取返回数据
     * @param database
     * @param table
     * @param fileds
     * @param idConds
     * @return
     * @throws Exception
     */
    private static List<List<String>> getDataById(String database, String table, List<String> fileds, List<ColumnCondition> idConds, String groupBy ) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(database, table);
        ColumnDef[] cols = tableInfo.getColumnDefs();
        cols = getSelColumn(cols, fileds);
        if (cols == null) {
            return new ArrayList<List<String>>();
        }
        if(cols.length > 0 && cols[0].getAggregation() >= 10) {
            //return getDataBySecondAggregation(database, table, cols, idConds, groupBy, true);
            return new ArrayList<List<String>>();
        } else if(cols.length > 0 && cols[0].getAggregation() > 0) {
            //return getDataByAggregation(database, table, cols, idConds);
            //return getDataBySecondAggregation(database, table, cols, idConds, groupBy, false);
            return new ArrayList<List<String>>();
        } else {
            return getDataByField(database, table, cols, idConds);
        }
    }

    /**
     * 根据ID 获取返回数据
     * @param database
     * @param table
     * @param fileds
     * @param idConds
     * @return
     * @throws Exception
     */
    private static Map<String, List<FunctionData>> getAggregationDataById(String database, String table, List<String> fileds, List<ColumnCondition> idConds, String groupBy ) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(database, table);
        ColumnDef[] cols = tableInfo.getColumnDefs();
        cols = getSelColumn(cols, fileds);
        if (cols == null) {
            return new ConcurrentSkipListMap<>();
        }
        if(cols.length > 0 && cols[0].getAggregation() >= 10) {
            return getDataBySecondAggregation(database, table, cols, idConds, groupBy, true);
        } else if(cols.length > 0 && cols[0].getAggregation() > 0) {
            //return getDataByAggregation(database, table, cols, idConds);
            return getDataBySecondAggregation(database, table, cols, idConds, groupBy, false);
        } else {
            //return new ConcurrentSkipListMap<>();
            return getDataBySecondAggregation(database, table, cols, idConds, groupBy, false);
        }
    }

    private static int getAggregation(String database, String table, List<String> fileds) throws Exception {
        TableInfo tableInfo = CacheUtil.readTableInfo(database, table);
        ColumnDef[] cols = tableInfo.getColumnDefs();
        cols = getSelColumn(cols, fileds);
        if (cols == null) {
            return -1;
        }
        if (cols.length == 0) {
            return -1;
        }
        return cols[0].getAggregation();
    }

    /**
     * 聚合函数计算数据
     * @param func 聚合函数工具
     * @param s 追加的数据
     */
    private static void addFunctionData(FunctionData func, String s) {
        func.addData(s);
    }

    /**
     * 根据聚合函数，获取返回数据
     * @param database
     * @param table
     * @param cols
     * @param idConds
     * @return
     * @throws Exception
     */
    private static Map<String, List<FunctionData>> getDataBySecondAggregation(final String database, final String table,
                                                                 ColumnDef[] cols, List<ColumnCondition> idConds, String groupBy, boolean useTimeWindow) throws Exception {
        List<Map<String, List<FunctionData>>> resultMiddle = new ArrayList<>();
        List<Future<Map<String, List<FunctionData>>>> futureList = new ArrayList<>();
        //Map<Integer, Future<List<String>>> groupByMap = new HashMap<>();
        //Map<Integer, List<String>> groupByMap = new HashMap<>();
        int k = idConds.size(); //结果条件size
        int k2 = cols.length;   //列长度
        TableInfo tbInfo = CacheUtil.readTableInfo(database, table);
        final ColumnDef orderColumn = tbInfo.getOrderColumn();

        final Map<Integer, ColumnDef> colMap = new HashMap<>() ;
        Integer j = 0;
        StringBuilder queryAggTypeBuilder = new StringBuilder("-");
        colMap.put(j++, orderColumn);
        for (ColumnDef cf : cols) {
            colMap.put(j++, cf);
            queryAggTypeBuilder.append(cf.getName()).append("-").append(cf.getAggregation()).append("-");
            orderColumn.setWindowSecond(cf.getWindowSecond());  //
        }
        queryAggTypeBuilder.append(groupBy).append("-");    //缓存要区分groupby
        queryAggTypeBuilder.append(orderColumn.getWindowSecond()).append("-");  //缓存要区分窗口
        final String queryAggType = queryAggTypeBuilder.toString();
        int colSize = colMap.size();
        long start = System.currentTimeMillis();
        final String[] gps = CommonUtil.isEmpty(groupBy) ? new String[]{} : groupBy.replaceAll("\\s", "").split(",");
        for (int i = 0; i < k; i++) {
            final int iFinal = i;
            Future<Map<String, List<FunctionData>>> f = PoolUtil.QUERY_FIX_POLL_GETDATA.submit(new Callable<Map<String, List<FunctionData>>>() {
                @Override
                public Map<String, List<FunctionData>> call() throws Exception {
                    //long st = System.currentTimeMillis();
                    //long stsub = st;
                    final ColumnCondition con = idConds.get(iFinal);
                    //需要重置start, limit
                    con.setStart(0);
                    con.setLimit((int) ServerConstants.PARK_SIZ);
                    DataPack dir = con.getDataDirLsit().get(0);

                    //走缓存模式
                    String colUsedCache = dir.getCol(); //随便找一个列了，当更新时，所有列都会更新
                    final String dirName = dir.getDirName();
                    //boolean useCache = queryCacheColumns.contains(colUsedCache);
                    boolean useCache = Params.isAggregationCache();    //聚合后的结果数据 根据参数 决定是否 启用缓存模式
                    String countKey = null;
                    if (useCache) { //使用缓存的列
                        countKey = con.toCountKey();
                        countKey = countKey.replaceFirst(BaseCache.SPLIT + "id" + BaseCache.SPLIT, BaseCache.SPLIT + colUsedCache + BaseCache.SPLIT);
                        //这里加缓存
                        Map<String, List<FunctionData>> resInCache = QueryCache.getSecondAggregationData(countKey, dirName, queryAggType);
                        if (resInCache != null) {
                            return resInCache;
                        }
                    }

                    Map<String, List<FunctionData>> packResult = new HashMap<>(); //单pack的结果数据
                    Map<Integer, List<String>> lineMap = new HashMap<>();
                    int orderSize = 0;
                    for (int i2 = 0; i2 <= k2; i2++) {
                        ColumnDef colQuery = null;
                        if (i2 == 0) { //第一列查询分区字段
                            colQuery = orderColumn;
                        } else {
                            final ColumnDef colDef = cols[i2 - 1];
                            colQuery = colDef;
                        }
                        final ColumnDef colQueryFinal = colQuery;
                        if (ColumnDef.isCount(colQueryFinal.getAggregation())) {
                            List<String> ls = new ArrayList<>(orderSize);
                            for (int i = 0; i < orderSize; i++) {
                                ls.add(null);
                            }
                            lineMap.put(i2, ls);
                        } else {
                            List<String> ls = queryColumnByIdAndCache(database, table, con, colQueryFinal);
                            if (i2 == 0) {
                                orderSize = ls.size();
                            }
                            lineMap.put(i2, ls);
                        }

                    }
                    //queryTimeLogger.debug("aggregation one step1 time:" + (System.currentTimeMillis() - stsub));
                    //stsub = System.currentTimeMillis();

                    //获取group by 字段数据
                    List<String> groupDataList = null;
                    //String[] gps = groupBy.split(",");
                    for (String g : gps) {
                        g = g.trim();
                        final ColumnDef colQueryFinal = tbInfo.getColumnDef(g);
                        List<String> gpData = queryColumnByIdAndCache(database, table, con, colQueryFinal);
                        if (groupDataList == null) {
                            //这里需要拷贝数据，避免缓存数据被外部修改
                            groupDataList = gpData;
                        } else {
                            for (int i = 0, k = groupDataList.size(); i < k; i++) {
                                String s = groupDataList.get(i) + "," + gpData.get(i);
                                groupDataList.set(i, s);
                            }
                        }
                    }

                    //queryTimeLogger.debug("aggregation one step2 time:" + (System.currentTimeMillis() - stsub));
                    //stsub = System.currentTimeMillis();

                    List<List<String>> lineList = new ArrayList<>();
                    int lineSize = lineMap.get(0).size();
                    for (int iii = 0; iii < lineSize; iii++) {      //行
                        List<String> line = new ArrayList<>();
                        for (int i2 = 0; i2 <= k2; i2++) {  //列转行
                            line.add(lineMap.get(i2).get(iii));
                        }
                        lineList.add(line);
                    }

                    //queryTimeLogger.debug("aggregation one step3 time:" + (System.currentTimeMillis() - stsub));
                    //stsub = System.currentTimeMillis();

                    Map<Integer, Map<String, FunctionData>> funcMap = new ConcurrentSkipListMap<>();
                    for (int ii = 0, kk = lineList.size(); ii < kk; ii++) {
                        List<String> line = lineList.get(ii);
                        if (line.isEmpty()) {
                            continue;
                        }
                        //Long time = 0L; //肯定是第一个列，由前面的逻辑保证
                        String timeGroup = null;    //追加分组功能
                        for (int iii = 0, kkk = line.size(); iii < kkk; iii++) {
                            String s  = line.get(iii);
                            ColumnDef col = colMap.get(iii);
                            Map<String, FunctionData> funcDataMap = funcMap.get(iii);
                            if (funcDataMap == null) {
                                funcDataMap = new ConcurrentSkipListMap<>();
                                funcMap.put(iii, funcDataMap);
                            }
                            if (iii == 0) {    //第一个分区列不管
                                if (useTimeWindow) {
                                    int windowSecond = col.getWindowSecond() > 0 ? col.getWindowSecond() * 1000 : 1000;
                                    long time = CommonUtil.parseLong(s) / windowSecond * windowSecond;
                                    timeGroup = String.valueOf(time);
                                } else {
                                    timeGroup = "";
                                }
                                if (groupDataList != null) {
                                    String groupData = groupDataList.get(ii);
                                    if (timeGroup.length() == 0) {
                                        timeGroup += groupData;
                                    } else {
                                        timeGroup += "=" + groupData;
                                    }
                                }
                                continue;
                            }

                            FunctionData func = funcDataMap.get(timeGroup);
                            if (func == null) {
                                String second = null;
                                String colName = col.getName();
                                int idx = -999;
                                for (int i = 0, k = gps.length; i < k; i++) {
                                    String g = gps[i];
                                    if (g.equals(colName)) {
                                        idx = i;
                                        break;
                                    }
                                }
                                //如果使用窗口模式，分组会多一段时间
                                if (useTimeWindow) {
                                    idx++;
                                }
                                String[] timeGps = timeGroup.split(",");
                                if (idx >= 0 && timeGps.length > idx) {
                                    second = timeGps[idx];
                                }
                                func = new FunctionData(second, col.getAggregation());
                                funcDataMap.put(timeGroup, func);
                            }
                            addFunctionData(func, s);
                        }
                    }

                    //queryTimeLogger.debug("aggregation one step4 time:" + (System.currentTimeMillis() - stsub));
                    //stsub = System.currentTimeMillis();

                    Map<String, FunctionData> secondMap = funcMap.get(1);
                    for (Map.Entry<String, FunctionData> secondEntry : secondMap.entrySet()) {
                        String second = secondEntry.getKey();
                        if (second.startsWith("0")) {  //过滤非法数据
                            continue;
                        }
                        List<FunctionData> list = new ArrayList<>();
                        for (int i3 = 0; i3 < colSize; i3++) {
                            if (i3 == 0) {
                                list.add(new FunctionData(second));
                                continue;   //第一列，不做聚合
                            }
                            FunctionData funcData = funcMap.get(i3).get(second);
                            /*int aggregation = colMap.get(i3).getAggregation();
                            String value = "0";
                            if (ColumnDef.SECOND_COUNT == aggregation) {
                                value = String.valueOf(funcData.getCount());
                            } else if (ColumnDef.SECOND_SUM == aggregation) {
                                value = funcData.getSum().toString();
                            } else if (ColumnDef.SECOND_MIN == aggregation) {
                                value = String.valueOf(funcData.getMin());
                            } else if (ColumnDef.SECOND_MAX == aggregation) {
                                value = String.valueOf(funcData.getMax());
                            }*/
                            list.add(funcData);
                        }
                        packResult.put(list.get(0).getSecond(), list);
                    }
                    if (useCache) { //这里加缓存
                        QueryCache.putSecondAggregationData(countKey, dirName, queryAggType, packResult);
                    }

                    //queryTimeLogger.debug("aggregation one step5 time:" + (System.currentTimeMillis() - stsub));
                    //stsub = System.currentTimeMillis();

                    //queryTimeLogger.debug("aggregation one time:" + (System.currentTimeMillis() - st) + ", size:" + orderSize);
                    return packResult;
                }
            });
            futureList.add(f);
        }

        for (Future<Map<String, List<FunctionData>>> f : futureList) {
            resultMiddle.add(f.get());
        }
        long end = System.currentTimeMillis();
        long timeTotal = end - start;
        queryTimeLogger.debug("find aggregation data time:" + timeTotal + ", size:" + futureList.size());
        start = end;
        Map<String, List<FunctionData>> mapChange = new ConcurrentSkipListMap<>();

        for (Map<String, List<FunctionData>> mapInPack : resultMiddle) {
            for (Map.Entry<String, List<FunctionData>> entry : mapInPack.entrySet()) {
                String key = entry.getKey();
                List<FunctionData> lineInChange = mapChange.get(key);
                if (lineInChange == null) { //第一次就直接赋值作为基准数据
                    mapChange.put(key, (List) ((ArrayList) entry.getValue()).clone());
                    continue;
                }
                List<FunctionData> datas = entry.getValue();
                for (int i3 = 0; i3 < colSize; i3++) {
                    FunctionData dd = datas.get(i3);
                    if (i3 == 0) {
                        //lineInChange.set(0, dd);
                        continue;   //第一列，不做聚合
                    }
                    int aggregation = colMap.get(i3).getAggregation();
                    /*long data = CommonUtil.parseLong(lineInChange.get(i3));
                    if (ColumnDef.SECOND_COUNT == aggregation) {
                        data += CommonUtil.parseLong(dd);
                    } else if (ColumnDef.SECOND_SUM == aggregation) {
                        data += CommonUtil.parseLong(dd);
                    } else if (ColumnDef.SECOND_MIN == aggregation) {
                        data = Math.min(data, CommonUtil.parseLong(dd));
                    } else if (ColumnDef.SECOND_MAX == aggregation) {
                        data = Math.max(data, CommonUtil.parseLong(dd));
                    } else if (ColumnDef.SECOND_DELTA == aggregation) {
                        //data = Math.max(data, CommonUtil.parseLong(dd));
                    }*/
                    FunctionData data = lineInChange.get(i3);
                    mergerFunctionData(data, dd, aggregation);
                    //lineInChange.set(i3, data);   对象地址传递就不用再赋值了
                }
            }
        }

        queryTimeLogger.debug("aggregation data step2 time:" + (System.currentTimeMillis() - start));
        return mapChange;
    }

    /**
     * 用于聚合函数数据合并
     * @param data  主数据
     * @param dd    被合并的数据
     * @param aggregation   聚合类型
     */
    public static void mergerFunctionData(FunctionData data, FunctionData dd, int aggregation) {
        if (ColumnDef.isCount(aggregation)) {
            data.addCount(dd.getCount());
        } else if (ColumnDef.isCountDistinct(aggregation)
                || ColumnDef.isDistinct(aggregation)) {
            data.addDistinct(dd.getDistinctSet());
        } else if (ColumnDef.isAvg(aggregation)) {
            data.addSum(dd.getSum());
            data.addCount(dd.getCount());
        } else if (ColumnDef.isSum(aggregation)) {
            data.addSum(dd.getSum());
        } else if (ColumnDef.isMin(aggregation)) {
            data.addMin(dd.getMin());
        } else if (ColumnDef.isMax(aggregation)) {
            data.addMax(dd.getMax());
        } else if (ColumnDef.SECOND_DELTA == aggregation) {
            data.addDelta(dd.getDeltaEnd());
        }
    }

    public static List<List<String>> resultMap2List(Map<String, List<FunctionData>> mapChange) {
        List<List<String>> result = new ArrayList<List<String>>();
        for (Map.Entry<String, List<FunctionData>> entry : mapChange.entrySet()) {
            List<List<String>> listAll = new ArrayList<>();
            List<String> list = new ArrayList<>(entry.getValue().size());
            listAll.add(list);
            for (FunctionData fd : entry.getValue()) {
                if (fd.getSecond() != null) {   //第一个跳过
                    list.add(fd.getSecond());
                } else {
                    list.add(fd.getData()); //正常逻辑，非distinct
                    if (ColumnDef.isDistinct(fd.getAggregation())) {
                        String second = list.get(0);
                        listAll.clear();    //清空，走额外逻辑
                        for (String s : fd.getDistinctSet()) {
                            List<String> listOne = new ArrayList<>(2);
                            listOne.add(second);
                            listOne.add(s);
                            listAll.add(listOne);
                        }

                    }
                }
            }
            result.addAll(listAll);
        }
        return result;
    }

    /**
     * 根据聚合函数，获取返回数据
     * @param database
     * @param table
     * @param cols
     * @param idConds
     * @return
     * @throws Exception
     */
    private static List<List<String>> getDataByAggregation(final String database, final String table, ColumnDef[] cols, List<ColumnCondition> idConds) throws Exception {
        List<List<String>> result = new ArrayList<List<String>>();
        Map<Integer, Map<Integer, Future<FunctionData>>> futrueMap = new HashMap<Integer, Map<Integer, Future<FunctionData>>>();
        int k = idConds.size(); //结果条件size
        int k2 = cols.length;   //列长度
        for (int i = 0; i < k; i++) {
            final ColumnCondition con = idConds.get(i);
            //recalculate(con);  //获取 聚合函数 数值 不需要重新计算
            //需要重置start, limit
            con.setStart(0);
            con.setLimit((int) ServerConstants.PARK_SIZ);
            //行查询 Future Map
            Map<Integer, Future<FunctionData>> lineMap = futrueMap.get(i);
            if (lineMap == null) {
                lineMap = new HashMap<Integer, Future<FunctionData>>();
                futrueMap.put(i, lineMap);
            }
            for (int i2 = 0; i2 <= k2; i2++) {
                if (i2 == 0) { //第一列存放 rowNumber好了(这玩意可以作为ID)
                    Future<FunctionData> f = PoolUtil.QUERY_FIX_POLL_GETDATA.submit(new Callable<FunctionData>() {
                        @Override
                        public FunctionData call() throws Exception {
                            return new FunctionData();
                        }
                    });
                    lineMap.put(i2, f);
                } else {
                    final ColumnDef colDef = cols[i2 - 1];
                    //列查询 Future
                    Future<FunctionData> f = PoolUtil.QUERY_FIX_POLL_GETDATA.submit(new Callable<FunctionData>() {
                        @Override
                        public FunctionData call() throws Exception {
                            FunctionData data = null;
                            int startNum = (int) (con.getQueryRanges().get(0).getStart() % ServerConstants.PARK_SIZ);
                            int endNum = (int) (con.getQueryRanges().get(0).getEnd() % ServerConstants.PARK_SIZ);
                            if (con.getQueryRanges().size() == 1
                                    && startNum == 0
                                    && endNum == ServerConstants.PARK_SIZ - 1) {  //强相关走索引
                                IndexInfo indexInfo = CacheUtil.getIndexInfo(database, table, con.getDataDirLsit().get(0).getDirName(), colDef.getName());
                                if (indexInfo == null) {
                                    data = new FunctionData(0, 0, new BigDecimal(0), 0, 0);
                                } else {
                                    int count = indexInfo.getCount();
                                    int notNullCount = count - indexInfo.getNullCount();
                                    BigDecimal sum = indexInfo.getSum();
                                    long min = indexInfo.getStart();
                                    long max = indexInfo.getEnd();
                                    data = new FunctionData(count, notNullCount, sum, min, max);
                                }
                            } else {    //不是强相关，需要查询数据
                                if (colDef.getAggregation() == ColumnDef.AGGREGATION_COUNT) {   //如果只是找count， 那么不需要查询，只需要遍历一下即可
                                    int count = 0;
                                    for (QueryRange qr : con.getQueryRanges()) {
                                        for (long i = qr.getStart(), k = qr.getEnd(); i <= k; i++) {
                                            count++;
                                        }
                                    }
                                    data = new FunctionData(count, 0, new BigDecimal(0), 0, 0);
                                } else {
                                    List<String> dataList = queryColumnById(colDef.getName(), con);
                                    int count = dataList.size();
                                    int notNullCount = 0;
                                    BigDecimal sum = new BigDecimal(0);
                                    long min = Long.MAX_VALUE;
                                    long max = Long.MIN_VALUE;
                                    for (String d : dataList) {
                                        if (d == null) {
                                            continue;
                                        }
                                        notNullCount++;
                                        if (colDef.isLong()) {
                                            long dd = 0;
                                            if (ServerConstants.USE_64) {
                                                dd = Convert10To64.unCompressNumberByLine(d);
                                            } else {
                                                dd = CommonUtil.parseInt(d);
                                            }
                                            sum = sum.add(new BigDecimal(dd));
                                            min = Math.min(min, dd);
                                            max = Math.max(max, dd);
                                        }
                                    }
                                    data = new FunctionData(count, notNullCount, sum, min, max);
                                }
                            }
                            return data;
                        }
                    });
                    lineMap.put(i2, f);
                }
            }
        }
        List<String> line = new ArrayList<String>();    //单行记录
        for (int i2 = 0; i2 <= k2; i2++) {
            if (i2 == 0) {  //第一列，行号
                line.add("0");  //行号
                continue;
            }
            int count = 0;
            int notNullCount = 0;
            BigDecimal sum = new BigDecimal(0);
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            final ColumnDef colDef = cols[i2 - 1];
            for (int i = 0; i < k; i++) {
                FunctionData data = futrueMap.get(i).get(i2).get();
                count += data.getCount();
                notNullCount += data.getNotNullCount();
                sum = sum.add(data.getSum());
                min = Math.min(min, data.getMin());
                max = Math.max(max, data.getMax());
            }
            if (min == Long.MAX_VALUE) {
                min = 0;
            }
            if (max == Long.MIN_VALUE) {
                max = 0;
            }
            if (colDef.getAggregation() == ColumnDef.AGGREGATION_COUNT) {
                line.add(String.valueOf(count));
            } else if (colDef.getAggregation() == ColumnDef.AGGREGATION_SUM) {
                line.add(sum.toString());
            } else if (colDef.getAggregation() == ColumnDef.AGGREGATION_AVG) {
                BigDecimal avg =sum.divide(new BigDecimal(notNullCount), 2, RoundingMode.HALF_UP);
                line.add(avg.toString());
            } else if (colDef.getAggregation() == ColumnDef.AGGREGATION_MIN) {
                line.add(String.valueOf(min));
            } else if (colDef.getAggregation() == ColumnDef.AGGREGATION_MAX) {
                line.add(String.valueOf(max));
            } else {
                queryTimeLogger.warn("aggregation is invalid, aggregation:" + colDef.getAggregation());
                line.add("0");
            }
        }
        result.add(line);   //集合函数，结果单行记录
        return result;
    }

    /**
     * 根据查找字段，获取返回数据
     * @param database
     * @param table
     * @param cols
     * @param idConds
     * @return
     * @throws Exception
     */
    private static List<List<String>> getDataByField(String database, String table, ColumnDef[] cols, List<ColumnCondition> idConds) throws Exception {
        List<List<String>> result = new ArrayList<List<String>>();
        Map<Integer, Map<Integer, Future<List<String>>>> futrueMap = new HashMap<Integer, Map<Integer, Future<List<String>>>>();
        int k = idConds.size(); //结果条件size
        int k2 = cols.length;   //列长度
        for (int i = 0; i < k; i++) {
            final ColumnCondition con = idConds.get(i);
            recalculate(con);  //根据start重新计算查询条件
            //行查询 Future Map
            Map<Integer, Future<List<String>>> lineMap = futrueMap.get(i);
            if (lineMap == null) {
                lineMap = new HashMap<Integer, Future<List<String>>>();
                futrueMap.put(i, lineMap);
            }
            for (int i2 = 0; i2 <= k2; i2++) {
                if (i2 == 0) { //最后一列存放 rowNumber好了(这玩意可以作为ID)
                    Future<List<String>> f = PoolUtil.QUERY_FIX_POLL_GETDATA.submit(new Callable<List<String>>() {
                        @Override
                        public List<String> call() throws Exception {
                            return queryRowNumber(con);
                        }
                    });
                    lineMap.put(i2, f);
                } else {
                    final ColumnDef colDef = cols[i2 - 1];
                    //列查询 Future
                    Future<List<String>> f = PoolUtil.QUERY_FIX_POLL_GETDATA.submit(new Callable<List<String>>() {
                        @Override
                        public List<String> call() throws Exception {
                            return queryColumnById(colDef.getName(), con);
                        }
                    });
                    lineMap.put(i2, f);
                }
            }
        }
        for (int i = 0; i < k; i++) {
            List<List<String>> dataInPack = new ArrayList<List<String>>();
            for (int i2 = 0; i2 <= k2; i2++) {
                ColumnDef cf = null;
                if (i2 == 0) {
                    cf = new ColumnDef(Constants.COLUMN_ID, ColumnDef.LONG_TYPE);
                } else {
                    cf = cols[i2 - 1];
                }
                final ColumnDef colDef = cf;
                EnumInfo colEnum = null;
                if (colDef.isEnum()) {
                    colEnum = CacheUtil.getEnumInfo(database, table, colDef.getName());
                }
                List<String> colList = futrueMap.get(i).get(i2).get();  //结果中如果为空，则用null 占位
                int countInPack = 0;    //属于第几行数据，根据此数值，获取对于行然后add
                for (String data : colList) {
                    if (!CommonUtil.isEmpty(data)) {    //数据不为空，则进行类型判定，进行数据修正
                        if (Constants.COLUMN_ID.equals(colDef.getName())) { //如果是 ID 列，不需要再转了
                            Long l = CommonUtil.parseLong2(data);
                            data = l == null ? null : String.valueOf(l);
                        } else if (colDef.isEnum()) {  //如果是枚举类型进行定义转化
                            Long l = null;
                            if (ServerConstants.USE_64) {
                                l = Convert10To64.unCompressNumberByLine(data);
                            } else {
                                l = CommonUtil.parseLong2(data);
                            }
                            if (l != null && colEnum != null) {
                                data = colEnum.getData(l);
                            }
                        } else if (colDef.isLong() || colDef.isDecimal()) {   //如果是字符串类型的也进行转化，主要是为了去0
                            if (colDef.getLength() > 0 || ServerConstants.USE_64) {
                                Long l = null;
                                if (ServerConstants.USE_64) {
                                    l = Convert10To64.unCompressNumberByLine(data);
                                } else {
                                    l = CommonUtil.parseLong2(data);
                                }
                                data = l == null ? null : String.valueOf(l);
                            }
                        } else if (colDef.isString()) { //数值类型, 并且有固定长度
                            if (colDef.getLength() > 0) {
                                data = data.replace("\0", "");
                            }
                        } else {
                            queryTimeLogger.warn("column type is invalid, col:" + colDef.toString());
                        }
                    }
                    List<String> lines = null;
                    if (dataInPack.size() > countInPack) {
                        lines = dataInPack.get(countInPack);
                    } else {
                        lines = new ArrayList<String>();
                        dataInPack.add(lines);
                    }
                    lines.add(data);
                    countInPack++;
                }
            }
            result.addAll(dataInPack);
        }
        return result;
    }

    private static ColumnDef[] getSelColumn(ColumnDef[] cols, List<String> fileds) throws BusinessException {
        if (CommonUtil.isEmpty(fileds)) {
            return cols;
        }
        if (fileds.size() == 1 && Constants.COLUMN_ID.equals(fileds.get(0))) {
            return new ColumnDef[0];
        }
        List<ColumnDef> selList = new ArrayList<ColumnDef>();
        Integer haveAggre = null;   //是否含有聚合方式
        for (String f : fileds) {
            int windowSecond = 0;
            int aggregation = 0;
            int idx = f.lastIndexOf("(");
            List<Integer> funcList = null;
            if (idx > -1) {
                String function = f.substring(0, idx + 1);
                funcList = getFunctions(function);
                aggregation = funcList.remove(0);   //获取第一个
                int winStartIdx = f.indexOf("[");
                int winEndIdx = f.indexOf("]");
                if (winStartIdx > -1 && winEndIdx > -1) {
                    windowSecond = CommonUtil.parseInt(f.substring(winStartIdx + 1, winEndIdx), 0);
                }
                int endIdx = f.indexOf(")");
                f = f.substring(idx + 1, endIdx).trim();

            }
            int haveAggEach = aggregation / 10;

            if (haveAggre == null) {
                haveAggre = haveAggEach;
            } else {
                if (!(haveAggre == haveAggEach)) {  //普通字段查询 和 聚合函数 和 按秒聚合 不能同时使用
                    throw new BusinessException("field select and aggregation and second aggregation function Cannot be used simultaneously");
                }
            }
            ColumnDef sel = null;
            for (ColumnDef col : cols) {
                if (f.equals(col.getName())) {
                    sel = col;
                    break;
                }
            }
            if (sel == null) {
                throw new BusinessException("select column is invalid, column:" + f);
            }
            //赋值聚合方式
            ColumnDef cloneDef = sel.clone();
            cloneDef.setAggregation(aggregation);
            cloneDef.setOtherAggregation(funcList);
            cloneDef.setWindowSecond(windowSecond);
            selList.add(cloneDef);
        }
        ColumnDef[] res = new ColumnDef[selList.size()];
        selList.toArray(res);
        return res;
    }

    /**
     * 嵌套函数处理
     * @param function
     * @return 返回的顺序是，先是最里层的函数类型，再是慢慢往外的层的函数类型
     * @throws BusinessException
     */
    private static List<Integer> getFunctions(String function) throws BusinessException {
        List<Integer> res = new ArrayList<>();
        int executeCount = 0;
        while (function.length() > 0 && executeCount++ < 10) {
            Holder<String> funcHolder = new Holder<>();
            int aggregation = getFunction(function, funcHolder);
            function = funcHolder.get();
            res.add(aggregation);
        }
        return res;
    }

    /**
     * 根据字符串解析出函数
     * @param function 函数字符
     * @param functionHolder 出参，剩余的函数字符
     * @return 函数类型
     * @throws BusinessException
     */
    private static int getFunction(String function, Holder<String> functionHolder) throws BusinessException {
        function = function.replaceAll("\\s", "");
        String funcOut = null;
        int aggregation = 0;
        if (function.endsWith("scount(distinct(")) {
            aggregation = ColumnDef.SECOND_COUNT_DISTINCT;
            funcOut = function.substring(0, function.length() - "scount(distinct(".length());
        } else if (function.endsWith("count(distinct(")) {
            aggregation = ColumnDef.AGGREGATION_COUNT_DISTINCT;
            funcOut = function.substring(0, function.length() - "count(distinct(".length());
        } else if (function.endsWith("scount(")) {
            aggregation = ColumnDef.SECOND_COUNT;
            funcOut = function.substring(0, function.length() - "scount(".length());
        } else if (function.endsWith("count(")) {
            aggregation = ColumnDef.AGGREGATION_COUNT;
            funcOut = function.substring(0, function.length() - "count(".length());
        } else if (function.endsWith("ssum(")) {
            aggregation = ColumnDef.SECOND_SUM;
            funcOut = function.substring(0, function.length() - "ssum(".length());
        } else if (function.endsWith("sum(")) {
            aggregation = ColumnDef.AGGREGATION_SUM;
            funcOut = function.substring(0, function.length() - "sum(".length());
        } else if (function.endsWith("savg(")) {
            aggregation = ColumnDef.SECOND_AVG;
            funcOut = function.substring(0, function.length() - "savg(".length());
        } else if (function.endsWith("avg(")) {
            aggregation = ColumnDef.AGGREGATION_AVG;
            funcOut = function.substring(0, function.length() - "avg(".length());
        } else if (function.endsWith("smin(")) {
            aggregation = ColumnDef.SECOND_MIN;
            funcOut = function.substring(0, function.length() - "smin(".length());
        } else if (function.endsWith("min(")) {
            aggregation = ColumnDef.AGGREGATION_MIN;
            funcOut = function.substring(0, function.length() - "min(".length());
        } else if (function.endsWith("smax(")) {
            aggregation = ColumnDef.SECOND_MAX;
            funcOut = function.substring(0, function.length() - "smax(".length());
        } else if (function.endsWith("max(")) {
            aggregation = ColumnDef.AGGREGATION_MAX;
            funcOut = function.substring(0, function.length() - "max(".length());
        } else if (function.endsWith("distinct(")) {
            aggregation = ColumnDef.AGGREGATION_DISTINCT;
            funcOut = function.substring(0, function.length() - "distinct(".length());
        } else if (function.endsWith("sdelta(")) {
            aggregation = ColumnDef.SECOND_DELTA;
            funcOut = function.substring(0, function.length() - "sdelta(".length());
        } else {
            throw new BusinessException("operate is invalid, operate:" + function);
        }
        if (functionHolder != null) {
            functionHolder.set(funcOut);
        }
        return aggregation;
    }

    /**
     * 根据start重新计算需要查询的数据
     * @param con
     */
    private static void recalculate(ColumnCondition con) {
        if (con.getStart() == 0 || CommonUtil.isEmpty(con.getQueryRanges())) {
            return;
        }
        int start = con.getStart();
        Iterator<QueryRange> it = con.getQueryRanges().iterator();
        while (it.hasNext()) {
            QueryRange qr = it.next();
            while (start > 0) {
                if (qr.getEnd() > qr.getStart()) {
                    qr.setStart(qr.getStart() + 1);
                    start--;
                } else if (qr.getEnd() == qr.getStart()) {
                    it.remove();
                    start--;
                    break;
                } else {
                    queryTimeLogger.error("QueryRange end > start, start:" + qr.getStart() + ", end:" + qr.getEnd());
                }
            }
            if (start <= 0) {
                break;
            }
        }
        con.setStart(start);
    }

    private static List<String> queryRowNumber(ColumnCondition con) {
        List<String> result = new ArrayList<String>();
        if (con == null || CommonUtil.isEmpty(con.getDataDirLsit())
                || CommonUtil.isEmpty(con.getQueryRanges())
                || con.getLimit() == 0) {
            return result;
        }
        DataPack dir = con.getDataDirLsit().get(0);
        int jump = con.getStart();   //需要根据start跳过的数据
        out:
        for (QueryRange qr : con.getQueryRanges()) {
            for (long i = qr.getStart(); i <= qr.getEnd(); i++) {
                if (--jump < 0) {
                    result.add(String.valueOf(i));
                    if (result.size() >= con.getLimit()) {
                        break out;
                    }
                }

            }
        }
        return result;
    }

    /**
     *
     * @param database
     * @param table
     * @param con
     * @param colQueryFinal
     * @return
     * @throws Exception
     */
    private static List<String> queryColumnByIdAndCache(String database, String table, ColumnCondition con, ColumnDef colQueryFinal) throws Exception {
        String col = colQueryFinal.getName();
        DataPack dir = con.getDataDirLsit().get(0);
        final String dirName = dir.getDirName();
        boolean useCache = queryCacheColumns.contains(col);
        //useCache = false;   //列查询先不用缓存
        String countKey = con.toCountKey();
        countKey = countKey.replaceFirst(BaseCache.SPLIT + "id" + BaseCache.SPLIT, BaseCache.SPLIT + col + BaseCache.SPLIT);

        if (useCache) { //使用缓存的列
            //这里加缓存
            List<String> resInCache = QueryCache.getColData(countKey, dirName);
            if (resInCache != null) {
                return resInCache;
            }
        }

        List<String> colList = queryColumnById(col, con);
        EnumInfo colEnum = null;
        if (colQueryFinal.isEnum()) {
            colEnum = CacheUtil.getEnumInfo(database, table, col);
        }
        for (int iii = 0, kkk = colList.size(); iii < kkk; iii++) {
            String data = colList.get(iii);
            if (!CommonUtil.isEmpty(data)) {    //数据不为空，则进行类型判定，进行数据修正
                if (colQueryFinal.isEnum()) {  //如果是枚举类型进行定义转化
                    Long l = null;
                    if (ServerConstants.USE_64) {
                        l = Convert10To64.unCompressNumberByLine(data);
                    } else {
                        l = CommonUtil.parseLong2(data);
                    }
                    if (l != null && colEnum != null) {
                        data = colEnum.getData(l);
                    }
                } else if (colQueryFinal.isLong() || colQueryFinal.isDecimal()) {   //如果是字符串类型的也进行转化，主要是为了去0
                    if (colQueryFinal.getLength() > 0 || ServerConstants.USE_64) {
                        Long l = null;
                        if (ServerConstants.USE_64) {
                            l = Convert10To64.unCompressNumberByLine(data);
                        } else {
                            l = CommonUtil.parseLong2(data);
                        }
                        data = l == null ? null : String.valueOf(l);
                    }
                } else if (colQueryFinal.isString()) { //数值类型, 并且有固定长度
                    if (colQueryFinal.getLength() > 0) {
                        data = data.replace("\0", "");
                    }
                } else {
                    queryTimeLogger.warn("column type is invalid, col:" + colQueryFinal.toString());
                }
                colList.set(iii, data);
            }
        }
        if (useCache) {
            QueryCache.putColData(countKey, dirName, colList);
        }
        return colList;
    }







    /**
     * 根据ID查询列
     * @param col  查询的列名
     * @param con   查询条件(id)
     * @return
     */
    private static List<String> queryColumnById(String col, ColumnCondition con) throws Exception {
        //return CommonUtil.asList(colDef.getName(), colDef.getName(), colDef.getName());
        //long s = System.currentTimeMillis();
        List<String> result = new ArrayList<String>();
        if (CommonUtil.isEmpty(con.getDataDirLsit()) || con.getLimit() == 0) {
            return result;
        }

        int size = con.getDataDirLsit().size();
        if (size > 0) {
            if (size != 1) {
                queryTimeLogger.warn("find column by id, dir is not eq 1.");
            }
            DataPack dir = con.getDataDirLsit().get(0);
            final String dirName = dir.getDirName();

            String space = getSpaceDir(dirName);
            byte[] bytes = CacheUtil.getDpData(con.getDataBase(), con.getTable(), space, dirName, col);
            List<String> qs = ByteDataUtil.searchById(bytes, con, dir, col);
            if (!CommonUtil.isEmpty(qs)) {
                result.addAll(qs);
            } else {
                queryTimeLogger.warn("get data is empty, col:" + col);
            }

        }
        //queryTimeLogger.debug("find-col[" + col + "]-data:" + (System.currentTimeMillis() - s));
        return result;
    }

    /**
     * 合并数据取交集
     * resultList1 和 resultList2 的 size 应该是一致的
     * @param resultList1
     * @param resultList2
     * @return
     */
    private static List<QueryResult> intersectionResult(List<QueryResult> resultList1, List<QueryResult> resultList2) {
        List<QueryResult> result = new ArrayList<QueryResult>();
        if (CommonUtil.isEmpty(resultList1) || CommonUtil.isEmpty(resultList2)) {
            queryTimeLogger.warn("resultList1 or resultList1 is null");
            return result;
        }
        int i = 0;
        int i2 = 0;
        int k = resultList1.size();
        int k2 = resultList2.size();
        while (i < k && i2 < k2) {
            QueryResult result1 = resultList1.get(i);
            QueryResult result2 = resultList2.get(i2);
            if (result1 == null || result1.getLineRangeList() == null) {
                i++;
                continue;
            }
            if (result2 == null || result2.getLineRangeList() == null) {
                i2++;
                continue;
            }
            int cmp = result1.getDir().getDirName().compareTo(result2.getDir().getDirName());
            if (cmp < 0) {
                i++;
                continue;
            } else if (cmp > 0) {
                i2++;
                continue;
            } else {
                List<LineRange> range1 = result1.getLineRangeList();
                List<LineRange> range2 = result2.getLineRangeList();
                List<LineRange> inner = intersectionRange(range1, range2);
                //计算total
                long total = 0L;
                for (LineRange r : inner) { //计算总数量
                    total += r.getEnd() - r.getStart() + 1;
                }
                //有些信息已经用不到了，例如是否强相关，所以无所谓了，之前无所谓，现在有所谓了，因为开始使用集合函数了
                int relation = Math.min(result1.getDir().getRelation(), result2.getDir().getRelation());
                QueryResult innerRes = new QueryResult(total, new DataPack(result1.getDir().getDirName(), relation));
                innerRes.setLineRangeList(inner);
                result.add(innerRes);
                i++;
                i2++;
            }
        }
        return result;
    }

    private static List<LineRange> intersectionRange(List<LineRange> range1, List<LineRange> range2) {
        List<LineRange> result = new ArrayList<LineRange>();
        if (CommonUtil.isEmpty(range1) || CommonUtil.isEmpty(range1)) { //有一个为空肯定没交集
            return result;
        }
        int i = 0;
        int i2 = 0;
        int k = range1.size();
        int k2 = range2.size();
        while (i < k && i2 < k2) {
            LineRange r1 = range1.get(i);
            LineRange r2 = range2.get(i2);
            if (r1.getEnd() < r2.getStart()) {
                i++;
            } else if (r1.getStart() > r2.getEnd()) {
                i2++;
            } else {
                long start = Math.max(r1.getStart(), r2.getStart());
                long end = Math.min(r1.getEnd(), r2.getEnd());
                LineRange res = new LineRange(start, end);
                result.add(res);
                if (r1.getEnd() > r2.getEnd()) {
                    i2++;
                } else if (r1.getEnd() < r2.getEnd()) {
                    i++;
                } else {
                    i++;
                    i2++;
                }
            }
        }
        return result;
    }


    /**
     * 合并块
     * @param packRelation
     * @param packRelation2
     * @return
     */
    private static List<DataPack> intersection(List<DataPack> packRelation, List<DataPack> packRelation2) {
        List<DataPack> result = new ArrayList<DataPack>();
        int i = 0;
        int k = packRelation.size();
        int i2 = 0;
        int k2 = packRelation2.size();
        while (i < k && i2 < k2) {
            DataPack dataPack1 = packRelation.get(i);
            DataPack dataPack2 = packRelation2.get(i2);
            if (dataPack2.getDirName().compareTo(dataPack1.getDirName()) < 0) {
                i2++;
            } else if (dataPack2.getDirName().compareTo(dataPack1.getDirName()) > 0) {
                i++;
            } else {    //处理相交的可疑列，可疑列中可以对某个字段强相关.
                if (dataPack1.getRelation() < dataPack2.getRelation()) {
                    if (!CommonUtil.isEmpty(dataPack2.getStrongRelCols())) {
                        dataPack1.addAllStrongRelCols(dataPack2.getStrongRelCols());
                    }
                    result.add(dataPack1);
                } else {
                    if (!CommonUtil.isEmpty(dataPack1.getStrongRelCols())) {
                        dataPack2.addAllStrongRelCols(dataPack1.getStrongRelCols());
                    }
                    result.add(dataPack2);
                }
                i++;
                i2++;
            }
        }
        return result;
    }

    /**
     * 查询条数
     * @param condition
     * @return
     * @throws Exception
     */
    private static List<QueryResult> countData(final ColumnCondition condition) throws Exception {
        List<QueryResult> lines = search(condition, 0);
        return lines;
    }

    /**
     * 查询数据
     * @param condition
     * @return
     * @throws Exception
     */
    private static List<QueryResult> queryData(final ColumnCondition condition) throws Exception {
        List<QueryResult> lines = search(condition, 1);
        return lines;
    }

    /**
     * 查询数据
     * @param condition
     * @param type
     * @return
     * @throws Exception
     */
    private static List<QueryResult> search(final ColumnCondition condition, final int type) throws Exception {
        String table = condition.getTable();
        final String col = condition.getColumn();
        String rowFile = Params.getBaseDir() + condition.getDataBase() + "/" + table + "/rows/";
        List<QueryResult> lines = new ArrayList<QueryResult>();
        List<Future<QueryResult>> futureList = new ArrayList<Future<QueryResult>>();
        List<DataPack> dataDirList = condition.getDataDirLsit();
        if (dataDirList == null) {
            dataDirList = findPackRelation(condition, null);
        }
        long tableEnd = Long.MAX_VALUE;
        final TableInfo tableInfo = CacheUtil.readTableInfo(condition.getDataBase(), table);
        if (tableInfo != null) {
            tableEnd = tableInfo.getNextId() - 1;
        }
        final long tableEndUsed = tableEnd;
        for (final DataPack dir : dataDirList) {
            final String dirName = dir.getDirName();
            final String space = getSpaceDir(dirName);
            final String key = rowFile + space + "/" + dirName + "/" + col;

            Future<QueryResult> future = PoolUtil.QUERY_FIX_POLL_SEARCH.submit(new Callable<QueryResult>() {
                @Override
                public QueryResult call() throws Exception {
                    if (type == 0) {    //如果是查询数量
                        //String countKey = dirName + "=" + condition.toCountKey();
                        String key = condition.toCountKey();
                        IndexInfo info = CacheUtil.getIndexInfo(condition.getDataBase(), condition.getTable(), space, dirName, col);
                        if (info != null && condition.getQueryRanges() != null && condition.getQueryRanges().size() == 1) {
                            if (info.getStart() > 0 && info.getEnd() > 0) {
                                QueryRange queryRange = condition.getQueryRanges().get(0);
                                if (queryRange.getStart() < info.getStart() && queryRange.getEnd() > info.getEnd()) {   //这样说明查询条件全覆盖，缓存可以忽略范围查询
                                    key = condition.toCountKey(true);
                                }
                            }

                        }
                        //缓存
                        QueryResult resInCache = QueryCache.get(key, dirName);
                        if (resInCache != null) {   //如果缓存中有直接返回就OK
                            return resInCache;
                        }

                        if (info != null) {
                            ColumnDef colDef = tableInfo.getColumnDef(condition.getColumn());
                            if (dir.getRelation() == Constants.RELATION_TAG_ALL     //此DP对所有列强相关
                                    || (dir.getRelation() == Constants.QUERY_TYPE_EQUAL && dir.isStrongRelCol(col))) {
                                //QueryResult result = new QueryResult(new ArrayList<DataLine>(0), info.getCount() - info.getNullCount(), dir);
                                //强相关说明没有Null
                                QueryResult result = new QueryResult(info.getCount(), dir);
                                long start = CommonUtil.parseInt(dirName) * ServerConstants.PARK_SIZ;
                                long end = start + ServerConstants.PARK_SIZ - 1;
                                long endReal = Math.min(end, tableEndUsed);
                                result.addLineRangeList(new LineRange(start, endReal));
                                if (endReal != end) {   //重新计算count [当前块未满，或者当前块有未完整的行]
                                    result.setTotal(endReal - start + 1);
                                }
                                QueryCache.put(key, dirName, result);   //设置缓存
                                return result;
                            } else if (colDef.isEnum() && info.existEnumIndexes()) {  //如果是枚举, 并且是满块的 (因为满块才会记录枚举的分布情况)
                                int[] ls = info.getLineNums(condition); // 已经解压缩，但是还是递增数值
                                QueryResult result = getResultForEnum(ls, dirName, tableEndUsed, dir);
                                QueryCache.put(key, dirName, result);   //设置缓存
                                return result;
                            } else if (colDef.isReverseIndex() && condition.getType() == Constants.QUERY_TYPE_FULLTEXT_RETRIEVAL && info.isFull()) {   //如果是全文检索
                                String searchKey = condition.getSearchKey();
                                int index = Math.abs(searchKey.hashCode()) % ServerConstants.GROUP_SIZE;
                                String file = key + FileConfig.SPLIT_WORD_SUFFIX.replace(FileConfig.SPLIT_WORD_IDX, String.valueOf(index));
                                //这里没做缓存了，由于文件比较大，如果放堆内，缓存个数太多会造成内部长期占用，可用内存变少，导致频繁GC，个数太少又会被频繁LRU替换，如果放堆外内存，会对堆外内存动态调整缓存长度 的策略产生影响，并且操作系统层面会对文件做缓存，这里就不做缓存策略了吧
                                List<String> lines = FileUtil.readAll(file, false);
                                String findStr = null;
                                for (String s : lines) {
                                    if (s.startsWith(searchKey + ServerConstants.INDEX_FLAG)) {
                                        findStr = s.substring((searchKey + ServerConstants.INDEX_FLAG).length());
                                    }
                                }
                                int[] ils = null;
                                if (findStr == null) {
                                    ils = new int[0];
                                } else {
                                    String[] ls = findStr.split(ServerConstants.DESC_LINE_SPLIT);
                                    ils = new int[ls.length];
                                    for (int i = 0, k = ls.length; i < k; i++) {
                                        if (ServerConstants.USE_64) {
                                            ils[i] = (int) Convert10To64.unCompressNumberByLine(ls[i]);
                                        } else {
                                            ils[i] = Integer.parseInt(ls[i]);
                                        }
                                    }
                                }
                                QueryResult result = getResultForEnum(ils, dirName, tableEndUsed, dir);
                                QueryCache.put(key, dirName, result);   //设置缓存
                                return result;
                            }
                        }
                        byte[] bytes = CacheUtil.getDpData(condition.getDataBase(), condition.getTable(), space, dirName, col);
                        final byte[] bytesF = bytes;
                        QueryResult res = ByteDataUtil.count(bytesF, condition, dir);
                        QueryCache.put(key, dirName, res);   //设置缓存
                        return res;
                    } else if (type == 1) {
                        byte[] bytes = CacheUtil.getDpData(condition.getDataBase(), condition.getTable(), space, dirName, col);
                        final byte[] bytesF = bytes;
                        return ByteDataUtil.search(bytesF, condition, dir);
                    } else {    //目前没有第三种..下面代码跑不到
                        return null;
                    }
                }
            });
            futureList.add(future);
        }
        for (Future<QueryResult> f : futureList) {
            QueryResult q = f.get();
            List<LineRange> lineRanges = q.getLineRangeList();
            //修正数据，防止未完全插入的数据被查询到
            if (!CommonUtil.isEmpty(lineRanges)) {    //如果不为空，check一次
                boolean change = false;
                for (int i = lineRanges.size() - 1; i >= 0; i--) {
                    LineRange lr = lineRanges.get(i);
                    if (lr.getEnd() <= tableEndUsed) {
                        break;
                    }
                    if (lr.getStart() > tableEndUsed) {
                        lineRanges.remove(i);
                        change = true;  //发生修正变更
                        continue;
                    }
                    change = true; //发生修正变更
                    lr.setEnd(Math.min(lr.getEnd(), tableEndUsed));
                }
                if (change) {   //如果被修正了，那么重新计算total
                    queryTimeLogger.warn("query uncommit data, so to exclude");
                    int total = 0;
                    for (LineRange lr : lineRanges) {
                        total += lr.getEnd() - lr.getStart() + 1;
                    }
                    q.setTotal(total);
                }
            }
            lines.add(q);
        }
        return lines;
    }

    private static QueryResult getResultForEnum(int[] ls, String dirName, long tableEndUsed, DataPack dir) {
        List<LineRange> lineRangeList = new ArrayList<LineRange>();
        long start = CommonUtil.parseInt(dirName) * ServerConstants.PARK_SIZ;
        long end = start + ServerConstants.PARK_SIZ - 1;
        long endReal = Math.min(end, tableEndUsed);
        int count = 0;
        int last = 0;
        for (int i = 0, k = ls.length; i < k; i++) {
            int lineInPack = ls[i] + last;
            long l = start + lineInPack;
            if (l > endReal) {
                break;
            }
            int size = lineRangeList.size();
            if (size == 0) {
                lineRangeList.add(new LineRange(l, l));
            } else {
                if (ls[i] == 1) { //如果是连续的
                    lineRangeList.get(size - 1).setEnd(l);
                } else {
                    lineRangeList.add(new LineRange(l, l));
                }
            }
            count++;
            last = lineInPack;
        }

        QueryResult result = new QueryResult(count, dir);
        result.setLineRangeList(lineRangeList);
        return result;
    }

    /**
     * 查询可疑DP，和强相关DP
     * @param condition
     * @param dpList 前几个条件检索出的结果DP
     * @return
     * @throws Exception
     */
    private static List<DataPack> findPackRelation(final ColumnCondition condition, List<DataPack> dpList) throws Exception {
        if (Constants.COLUMN_ID.equals(condition.getColumn())) {    //如果是 ID列查询
            return findPackRelationByIdCondition(condition, dpList);
        }
        List<DataPack> dataDirList = new ArrayList<DataPack>();
        Set<String> spaceSet = null;
        Set<String> dirSet = null;
        if (dpList != null) {
            if (dpList.isEmpty()) { //如果被设置了，但是为空，则说明已经检索不到了，直接返回好了
                return dataDirList;
            }
            DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
            spaceSet = new HashSet<String>();
            dirSet = new HashSet<String>();
            for (DataPack dp : dpList) {
                String dir = dp.getDirName();
                dirSet.add(dir);
                long sp = Long.parseLong(dir) / Constants.SPACE_SIZ;
                spaceSet.add(df.format(sp));
            }
        }
        final Set<String> spSet  = spaceSet;
        final Set<String> drSet = dirSet;
        condition.calculateSpaceDir();
        String rowFile = Params.getBaseDir() + condition.getDataBase() + "/" + condition.getTable() + "/rows/";
        File file = new File(rowFile);
        int findSpaceCount = 0;
        for (final File space : file.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (spSet != null && !spSet.contains(name)) {
                    return false;
                }
                if (condition.getSpaceDir() != null && !condition.getSpaceDir().contains(name)) {
                    return false;
                }
                return true;
            }
        })) {
            if (space.isFile()) {
                continue;
            }
            String spaceName = space.getName();
            if (CommonManager.isOrderColumn(condition.getDataBase(), condition.getTable(), condition.getColumn())) {  //如果是表分区列，则读取分区索引
                SpaceInfo spaceInfo = CacheUtil.getSpaceInfo(condition.getDataBase(), condition.getTable(), space.getName(), condition.getColumn());
                if (spaceInfo != null && spaceInfo.isExsit(condition) == Constants.RELATION_TAG_NONE) {   //此列有表空间索引，并且索引没匹配当前数据，跳过
                    continue;
                }
            }
            findSpaceCount++;
            //queryTimeLogger.debug("find-space:" + spaceName);
            for (File f : space.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(FileConfig.INDEX_FILE_SUFFIX)) {
                        return false;
                    }
                    if (drSet != null && !drSet.contains(name)) {
                        return false;
                    }
                    if (condition.getDataDirLsit() != null && !dirExsit(condition.getDataDirLsit(), name)) {
                        return false;
                    }
                    return true;
                }
            })) {
                if (f.isFile()) {
                    continue;
                }
                String path = f.getPath();
                final String dirName = f.getName();
                //String key = path + "/" + condition.getColumn();
                //long s = System.currentTimeMillis();
                IndexInfo info = CacheUtil.getIndexInfo(condition.getDataBase(), condition.getTable(), f.getParentFile().getName(), dirName, condition.getColumn());
                //queryTimeLogger.debug("get dir index col:" + condition.getColumn() + ", dir:" + dirName + ", time:" + (System.currentTimeMillis() - s));
                if (info != null) {
                    int relation = info.isExsit(condition);
                    if (relation >= Constants.RELATION_TAG_MAYBE) {  //至少可能相关才会去查询
                        dataDirList.add(new DataPack(dirName, relation).setCol(condition.getColumn()));
                    }
                }
            }
        }
        queryTimeLogger.debug("[" + condition.getColumn() + "] " + "find space-size:" + findSpaceCount);
        Collections.sort(dataDirList, new Comparator<DataPack>() {
            @Override
            public int compare(DataPack o1, DataPack o2) {
                return o1.getDirName().compareTo(o2.getDirName());
            }
        });
        return dataDirList;
    }

    private static boolean dirExsit(List<DataPack> list, String dirName) {
        int lo = 0;
        int hi = list.size() - 1;
        int mid;
        while(lo <= hi){
            mid = (lo + hi) / 2;
            int cp = list.get(mid).getDirName().compareTo(dirName);
            if(cp == 0){
                return true;
            }else if(cp < 0){
                lo = mid + 1;
            }else {
                hi= mid - 1;
            }
        }
        return false;
    }

    private static String getSpaceDir(String dir) {
        DecimalFormat df = new DecimalFormat("0000000000");
        int spaceIdx = CommonUtil.parseInt(dir) / Constants.SPACE_SIZ;
        return df.format(spaceIdx);
    }

    /**
     * 根据ID列 查询可疑DP和强相关DP
     * @param condition
     * @param dpList 前几个条件检索出的结果DP
     * @return
     */
    private static List<DataPack> findPackRelationByIdCondition(ColumnCondition condition, List<DataPack> dpList) throws Exception {
        if (dpList != null && dpList.isEmpty()) { //如果被设置了，但是为空，则说明已经检索不到了，直接返回好了
            return dpList;
        }
        List<DataPack> dataDirList = new ArrayList<>();
        if (condition.getType() == Constants.QUERY_TYPE_EQUAL) {   //如果是ID查询，并且只有这一个条件，则特殊处理
            long id = -1;
            if (CommonUtil.isEmpty(condition.getSearchKey())) {
                id = condition.getSearch();
            } else {
                id = CommonUtil.parseLong(condition.getSearchKey(), -1);
            }
            DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
            long park = id / ServerConstants.PARK_SIZ;
            String dirName = df.format(park);
            //单个查询的必定是可疑相关
            dataDirList.add(new DataPack(dirName, Constants.RELATION_TAG_MAYBE).setCol(condition.getColumn()));
            if (dpList != null) {
                boolean find = false;   //假设没找到
                for (DataPack dp : dpList) {
                    if (dirName.equals(dp.getDirName())) {
                        find = true;
                        break;
                    }
                }
                if (!find) {    //如果没找到直接clear
                    dataDirList.clear();
                }
            }

        } else if (condition.getType() == Constants.QUERY_TYPE_RANGE) {
            QueryRange qr = condition.getQueryRanges().get(0);
            if (dpList != null) {
                for (Iterator<DataPack> it = dpList.iterator(); it.hasNext();) {
                    DataPack dp = it.next();
                    long start = Long.parseLong(dp.getDirName()) * ServerConstants.PARK_SIZ;
                    long end = start + ServerConstants.PARK_SIZ - 1;
                    if (qr.getStart() <= start && qr.getEnd() >= end) {
                        continue;
                    } else if (qr.getEnd() < start || qr.getStart() > end) {
                        it.remove();
                    } else {
                        dp.setRelation(Constants.RELATION_TAG_MAYBE);
                    }
                }
                dataDirList = dpList;
            } else {    //基于上面的代码逻辑，这个基本走不到，但是也要做一下，万一未来跑到了呢？
                DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
                for (long i = qr.getStart(); i <= qr.getEnd(); i += ServerConstants.PARK_SIZ) {
                    long park = i / ServerConstants.PARK_SIZ;
                    String dirName = df.format(park);
                    long partStart = park * ServerConstants.PARK_SIZ;
                    long partEnd = partStart + ServerConstants.PARK_SIZ - 1;
                    int relation = Constants.RELATION_TAG_MAYBE;
                    if (partStart >= qr.getStart() && partEnd <= qr.getEnd()) {
                        relation = Constants.RELATION_TAG_ALL;
                    }
                    //单个查询的必定是可疑相关
                    dataDirList.add(new DataPack(dirName, relation).setCol(condition.getColumn()));
                }
            }
        } else {
            throw new BusinessException("query type is invalid when query by id");
        }
        return dataDirList;
    }

    public static void main(String[] args) throws BusinessException {
        String f = "distinct(count(scount( distinct(time ))))[10]";
        int idx = f.lastIndexOf("(");
        String function = f.substring(0, idx + 1);
        List<Integer> aggrList = getFunctions(function);
        System.out.println(aggrList);
    }

}
