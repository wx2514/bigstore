package com.wuqing.client.bigstore.bean.pkg;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by wuqing on 18/9/7.
 * 结果监听类
 */
public interface ResultListener<T> {

    /**
     * 获取结果
     * 10秒超时
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    T getResult() throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * 设置结果
     * @param message
     */
    void setResult(T message);

}
