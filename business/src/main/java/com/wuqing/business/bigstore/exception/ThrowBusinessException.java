package com.wuqing.business.bigstore.exception;

/**
 * Created by wuqing on 17/4/11.
 * 需要往外抛的异常
 */
public class ThrowBusinessException extends Exception {

    public ThrowBusinessException() {
        super();
    }

    public ThrowBusinessException(String message) {
        super(message);
    }

    public ThrowBusinessException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThrowBusinessException(Throwable cause) {
        super(cause);
    }
}
