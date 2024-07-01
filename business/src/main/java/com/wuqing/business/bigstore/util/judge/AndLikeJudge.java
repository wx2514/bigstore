package com.wuqing.business.bigstore.util.judge;

import java.util.ArrayList;
import java.util.List;

public class AndLikeJudge implements Judge {

    List<String> searchList = new ArrayList<>();

    @Override
    public boolean isMath(byte[] key) throws Exception {
        for (String searchKey : searchList) {
            LikeJudge judge = new LikeJudge(searchKey);
            if (!judge.isMath(key)) {
                return false;
            }
        }
        return true;
    }

    public AndLikeJudge(List<String> searchList) throws Exception {
        this.searchList = searchList;
    }
}
