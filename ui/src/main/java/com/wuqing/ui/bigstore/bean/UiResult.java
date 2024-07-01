package com.wuqing.ui.bigstore.bean;

import java.util.HashMap;
import java.util.Map;

public class UiResult<T> {

    private boolean success = false;

    private long total;

    private T value;

    private String error;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setSuccessData(T value) {
        this.success = true;
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
