package com.wuqing.business.bigstore.util.filterjudge;

import com.wuqing.client.bigstore.bean.QueryRange;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FilterRangeJudge implements FilterJudge {

    private final static Logger logger = LoggerFactory.getLogger(FilterRangeJudge.class);

    private List<QueryRange> queryRanges;

    public FilterRangeJudge(List<QueryRange> queryRanges) {
        this.queryRanges = queryRanges;
    }

    @Override
    public boolean isMath(String data) {
        if (CommonUtil.isEmpty(queryRanges)) {
            logger.warn("queryRanges is empty");
            return false;   //如果查询条件有问题直接返回不匹配
        }
        Long dataLong = CommonUtil.parseLong2(data);
        if (dataLong == null) {
            return false;
        }
        for (QueryRange qr : queryRanges) {
            if (dataLong >= qr.getStart() && dataLong <= qr.getEnd()) {
                return true;
            }
        }
        return false;
    }
}
