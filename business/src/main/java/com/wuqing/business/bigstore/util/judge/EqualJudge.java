package com.wuqing.business.bigstore.util.judge;

import com.wuqing.business.bigstore.util.DataBaseUtil;
import com.wuqing.client.bigstore.config.Constants;

/**
 * Created by wuqing on 17/3/27.
 */
public class EqualJudge implements Judge {

    private byte[] searchByte = null;


    public EqualJudge(long search, int length) throws Exception {
        searchByte = DataBaseUtil.formatData(search, length, true).getBytes(Constants.DEFAULT_CHARSET);
    }

    public EqualJudge(String search, int length) throws Exception {
        searchByte = DataBaseUtil.formatData(search, length, false).getBytes(Constants.DEFAULT_CHARSET);
    }

    @Override
    public boolean isMath(byte[] key) {
        if (searchByte != null) {
            if (DataBaseUtil.compare(key, searchByte) == 0) {
                return true;
            }
        }
        return false;
    }


}
