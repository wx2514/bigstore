package com.wuqing.business.bigstore.bean;

/**
 * Created by wuqing on 16/7/14.
 */
public class Holder<T> {

    private T value;

    public T get() {
        return value;
    }

    public void set(T value) {
        this.value = value;
    }
}
