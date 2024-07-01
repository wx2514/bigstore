package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wuqing on 17/6/19.
 */
public class FunctionData implements Serializable {

    private final static Logger queryTimeLogger = LoggerFactory.getLogger("query-time-log");

    private static final long serialVersionUID = 1L;

    private String second;  //秒数

    private long count;

    private Set<String> distinctSet = new HashSet<>();

    private long notNullCount;

    private BigDecimal sum;

    private long min;

    private long max;

    private int aggregation;

    private long deltaStart = Long.MIN_VALUE;

    private long deltaEnd = Long.MIN_VALUE;

    public FunctionData(String second) {
        this.second = second;
    }

    public FunctionData() {
        this.sum = new BigDecimal(0);
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
    }

    public FunctionData(long count, long notNullCount, BigDecimal sum, long min, long max) {
        this.count = count;
        this.notNullCount = notNullCount;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    public FunctionData(int aggregation) {
        this.aggregation = aggregation;
        this.sum = new BigDecimal(0);
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
    }

    public FunctionData(String second, int aggregation) {
        this.second = second;
        this.aggregation = aggregation;
        this.sum = new BigDecimal(0);
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getNotNullCount() {
        return notNullCount;
    }

    public void setNotNullCount(long notNullCount) {
        this.notNullCount = notNullCount;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public long getDeltaStart() {
        return deltaStart;
    }

    public void setDeltaStart(long deltaStart) {
        this.deltaStart = deltaStart;
    }

    public long getDeltaEnd() {
        return deltaEnd;
    }

    public void setDeltaEnd(long deltaEnd) {
        this.deltaEnd = deltaEnd;
    }

    public int getAggregation() {
        return aggregation;
    }

    public void addData(String data) {
        int aggregation = this.getAggregation();
        if (ColumnDef.isCount(aggregation)) {
            this.addCount();
        } else if (ColumnDef.isCountDistinct(aggregation)
                || ColumnDef.isDistinct(aggregation)) {
            this.addDistinct(data);
        } else if (ColumnDef.isAvg(aggregation)) {
            this.addSum(CommonUtil.parseLong(data));
            this.addCount();
        } else if (ColumnDef.isSum(aggregation)) {
            this.addSum(CommonUtil.parseLong(data));
        } else if (ColumnDef.isMin(aggregation)) {
            this.addMin(CommonUtil.parseLong(data));
        } else if (ColumnDef.isMax(aggregation)) {
            this.addMax(CommonUtil.parseLong(data));
        } else if (ColumnDef.SECOND_DELTA == aggregation) {
            this.addDelta(CommonUtil.parseLong(data));
        }
    }

    public void addCount() {
        this.count++;
    }

    public void addCount(long count) {
        this.count += count;
    }

    public void addNotNullCount() {
        this.notNullCount++;
    }

    public void addSum(long data) {
        this.sum = this.sum.add(new BigDecimal(data));
    }

    public void addSum(BigDecimal data) {
        this.sum = this.sum.add(data);
    }

    public void addMin(long min) {
        this.min = Math.min(this.min, min);
    }

    public void addMax(long max) {
        this.max = Math.max(this.max, max);
    }

    public void addDelta(long value) {
        if (deltaStart == Long.MIN_VALUE) {
            deltaStart = value;
        }
        deltaEnd = value;
    }

    public Set<String> getDistinctSet() {
        return distinctSet;
    }

    public void addDistinct(String value) {
        distinctSet.add(value);
    }

    public void addDistinct(Set<String> valueSet) {
        distinctSet.addAll(valueSet);
    }

    public String getData() {
        String value = "0";
        if (ColumnDef.isCount(this.aggregation)) {
            value = String.valueOf(this.getCount());
        } else if (ColumnDef.isCountDistinct(this.aggregation)) {
            value = String.valueOf(this.getCountDistinct());
        } else if (ColumnDef.isAvg(this.aggregation)) {
            value = String.valueOf(this.getSum().divide(new BigDecimal(this.getCount()), 2, BigDecimal.ROUND_HALF_UP));
        } else if (ColumnDef.isSum(this.aggregation)) {
            value = String.valueOf(this.getSum());
        } else if (ColumnDef.isMin(this.aggregation)) {
            value = String.valueOf(this.getMin());
        } else if (ColumnDef.isMax(this.aggregation)) {
            value = String.valueOf(this.getMax());
        } else if (ColumnDef.SECOND_DELTA == this.aggregation) {
            value = String.valueOf(this.deltaEnd - this.deltaStart);
        } else {
            value = this.second;
            //queryTimeLogger.warn("aggregation is invalid, aggregation:" + this.aggregation);
        }
        return value;
    }

    public List<String> getDataList() {
        List<String> values = new ArrayList<>();
        if (ColumnDef.isDistinct(this.aggregation)) {
            values.addAll(this.distinctSet);
        }
        return values;
    }

    /**
     * 根据聚合类型，返回数据
     * @return
     */
    public List<String> getDataByAggregation() {
        List<String> values = new ArrayList<>();
        if (ColumnDef.isDistinct(this.aggregation)) {
            values.addAll(getDataList());
        } else {
            values.add(getData());
        }
        return values;
    }

    private long getCountDistinct() {
        return distinctSet.size();
    }
}
