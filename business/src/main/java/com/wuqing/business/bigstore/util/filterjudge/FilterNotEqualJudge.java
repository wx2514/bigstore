package com.wuqing.business.bigstore.util.filterjudge;

public class FilterNotEqualJudge implements FilterJudge {

    private FilterEqualJudge filterEqualJudge;

    public FilterNotEqualJudge(String search) {
        filterEqualJudge = new FilterEqualJudge(search);
    }

    @Override
    public boolean isMath(String data) {
        return !this.filterEqualJudge.isMath(data);
    }
}
