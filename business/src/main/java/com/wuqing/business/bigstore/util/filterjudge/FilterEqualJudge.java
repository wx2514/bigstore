package com.wuqing.business.bigstore.util.filterjudge;

public class FilterEqualJudge implements FilterJudge {

    private String search;

    public FilterEqualJudge(String search) {
        this.search = search;
    }

    @Override
    public boolean isMath(String data) {
        if (this.search == null) {
            if (data == null) {
                return true;
            } else {
                return false;
            }
        } else {
            return this.search.equals(data);
        }
    }
}
