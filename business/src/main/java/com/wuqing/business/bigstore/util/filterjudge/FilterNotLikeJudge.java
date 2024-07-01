package com.wuqing.business.bigstore.util.filterjudge;

public class FilterNotLikeJudge implements FilterJudge {

    private FilterLikeJudge filterLikeJudge;

    public FilterNotLikeJudge(String search) {
        this.filterLikeJudge = new FilterLikeJudge(search);
    }

    @Override
    public boolean isMath(String data) {
        return !this.filterLikeJudge.isMath(data);
    }
}
