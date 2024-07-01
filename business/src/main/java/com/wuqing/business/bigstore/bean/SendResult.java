package com.wuqing.business.bigstore.bean;

/**
 * Created by wuqing on 17/8/31.
 */
public class SendResult {
    private long lastTime = 0;

    private boolean allowMerger = false;

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public boolean isAllowMerger() {
        return allowMerger;
    }

    public void setAllowMerger(boolean allowMerger) {
        this.allowMerger = allowMerger;
    }
}
