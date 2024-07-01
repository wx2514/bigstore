package com.wuqing.business.bigstore.util.judge;

import java.util.ArrayList;
import java.util.List;

public class OrLikeJudge implements Judge {

    List<String> searchList = new ArrayList<>();

    @Override
    public boolean isMath(byte[] key) throws Exception {
        for (String search : searchList) {
            LikeJudge judge = new LikeJudge(search);
            if (judge.isMath(key)) {
                return true;
            }
        }
        return false;
    }

    public OrLikeJudge(List<String> searchList) throws Exception {
        this.searchList = searchList;
    }
}
