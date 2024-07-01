package com.wuqing.business.bigstore.util.judge;

import java.util.ArrayList;
import java.util.List;

public class InJudge implements Judge {

    List<EqualJudge> equalJudgeList = new ArrayList<>();

    @Override
    public boolean isMath(byte[] key) throws Exception {
        for (EqualJudge eqJudge : equalJudgeList) {
            if (eqJudge.isMath(key)) {
                return true;
            }
        }
        return false;
    }

    public InJudge(int length, String... searchs) throws Exception {
        for (String s : searchs) {
            equalJudgeList.add(new EqualJudge(s, length));
        }
    }

    public InJudge(int length, long... searchs) throws Exception {
        for (long l : searchs) {
            equalJudgeList.add(new EqualJudge(l, length));
        }
    }


}
