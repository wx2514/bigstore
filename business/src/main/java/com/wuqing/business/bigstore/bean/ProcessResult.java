package com.wuqing.business.bigstore.bean;

import java.util.List;

/**
 * Created by wuqing on 17/8/30.
 */
public class ProcessResult {
    private boolean success;

    private List<String> resList;


    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public List<String> getResList() {
        return resList;
    }

    public void setResList(List<String> resList) {
        this.resList = resList;
    }
}
