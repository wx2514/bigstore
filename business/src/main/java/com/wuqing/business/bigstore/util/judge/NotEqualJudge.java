package com.wuqing.business.bigstore.util.judge;

/**
 * Created by wuqing on 17/3/27.
 */
public class NotEqualJudge implements Judge {

    private EqualJudge equalJudge;


    public NotEqualJudge(long search, int length) throws Exception {
        this.equalJudge = new EqualJudge(search, length);
    }

    public NotEqualJudge(String search, int length) throws Exception {
        this.equalJudge = new EqualJudge(search, length);
    }

    @Override
    public boolean isMath(byte[] key) {
        return !this.equalJudge.isMath(key);
    }


}
