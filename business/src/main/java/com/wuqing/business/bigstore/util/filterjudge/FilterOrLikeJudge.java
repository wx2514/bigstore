package com.wuqing.business.bigstore.util.filterjudge;

import java.util.ArrayList;
import java.util.List;

public class FilterOrLikeJudge implements FilterJudge {

    private List<FilterLikeJudge> judgeList = new ArrayList<>();

    public FilterOrLikeJudge(List<String> list) {
        for (String s : list) {
            judgeList.add(new FilterLikeJudge(s));
        }
    }

    @Override
    public boolean isMath(String data) {
        if (data == null) {
            return false;
        }
        for (FilterLikeJudge judge : judgeList) {
            if (judge.isMath(data)) {
                return true;
            }
        }
        return false;
    }
}
