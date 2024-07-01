package com.wuqing.client.bigstore.hold;

import com.wuqing.client.bigstore.bean.Condition;

/**
 * Created by wuqing on 17/4/5.
 */
public class QueryTableHolder extends Holder {

    public QueryTableHolder(Condition condition) {
        this.condition = condition;
        type = QUERY;
    }

    /**
     * 是否查询集群所有节点
     */
    private boolean queryGroup;

    private Condition condition;

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public boolean isQueryGroup() {
        return queryGroup;
    }

    public void setQueryGroup(boolean queryGroup) {
        this.queryGroup = queryGroup;
    }
}
