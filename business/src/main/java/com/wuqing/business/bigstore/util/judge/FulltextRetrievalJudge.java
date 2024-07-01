package com.wuqing.business.bigstore.util.judge;

/**
 * Created by wuqing on 17/6/27.
 */
public class FulltextRetrievalJudge implements Judge {

    private String search;

    public FulltextRetrievalJudge(String search) {
        this.search = search;
    }

    @Override
    public boolean isMath(byte[] path) {
        return false;
    }

}
