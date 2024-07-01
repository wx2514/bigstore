package com.wuqing.business.bigstore.util.judge;

/**
 * Created by wuqing on 17/3/27.
 */
public class NotLikeJudge implements Judge {

    private LikeJudge likeJudge;

    public NotLikeJudge(String search) throws Exception {
        this.likeJudge = new LikeJudge(search);
    }

    @Override
    public boolean isMath(byte[] key) {
        return !this.likeJudge.isMath(key);
    }



}
