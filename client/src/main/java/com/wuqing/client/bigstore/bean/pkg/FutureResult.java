package com.wuqing.client.bigstore.bean.pkg;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 参照futureTask实现的future监听器类.
 * 去掉run方法,修改set方法.
 * set方法类似触发器,解除阻塞
 *
 * @author benchu
 * @version 15/10/10.
 */
public class FutureResult<T> extends FutureTask implements ResultListener<T> {

    private long timeOut = 60000;   //毫秒

    /*public FutureResult() {
        super(() -> null);
    }*/

    public FutureResult(long timeOut) {
        super(() -> null);
        this.timeOut = timeOut;
    }

    @Override
    public T getResult() throws InterruptedException, ExecutionException, TimeoutException {
        return (T) super.get(timeOut, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setResult(T message) {
        super.set(message);
    }
}
