package com.wuqing.client.bigstore.hold;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/5.
 */
public class FlushTableCacheHolder extends Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    public FlushTableCacheHolder() {
        type = Holder.FLUSH_TABLE_CACHE;
    }

}
