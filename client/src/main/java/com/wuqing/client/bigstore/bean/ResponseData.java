package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/6.
 */
public class ResponseData implements Serializable {
    private static final long serialVersionUID = 1L;

    private DataResult data;

    private boolean success;

    private String errorMsg;

    public DataResult getData() {
        return data;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setSuccessData(DataResult data) {
        this.data = data;
        this.success = true;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        this.success = false;
    }
}
