package com.wuqing.business.bigstore.util.filterjudge;

import java.util.ArrayList;
import java.util.List;

public class FilterInJudge implements FilterJudge {

    private List<FilterEqualJudge> judgeList;

    public FilterInJudge(String... searchs) {
        judgeList = new ArrayList<>();
        for (String s : searchs) {
            judgeList.add(new FilterEqualJudge(s));
        }
    }

    @Override
    public boolean isMath(String data) {
        for (FilterEqualJudge eqJudge : judgeList) {
            if (eqJudge.isMath(data)) {
                return true;
            }
        }
        return false;
    }
}
