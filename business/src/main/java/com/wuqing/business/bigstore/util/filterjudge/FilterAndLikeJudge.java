package com.wuqing.business.bigstore.util.filterjudge;

import java.util.ArrayList;
import java.util.List;

public class FilterAndLikeJudge implements FilterJudge {

    private List<FilterLikeJudge> judgeList = new ArrayList<>();

    public FilterAndLikeJudge(List<String> list) {
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
            if (!judge.isMath(data)) {
                return false;
            }
        }
        return true;
    }
}