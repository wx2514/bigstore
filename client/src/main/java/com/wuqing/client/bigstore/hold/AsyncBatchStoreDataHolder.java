package com.wuqing.client.bigstore.hold;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/4/5.
 */
public class AsyncBatchStoreDataHolder extends Holder implements Serializable {


    private static final long serialVersionUID = 1L;

    public AsyncBatchStoreDataHolder() {
        type = ASYNC_BATCH_STORE_DATA;
    }

    private List<AsyncStoreDataHolder> dataList = new ArrayList<>();

    public void add(AsyncStoreDataHolder data) {
        dataList.add(data);
    }

    public List<AsyncStoreDataHolder> getDataList() {
        return dataList;
    }

    public void setDataList(List<AsyncStoreDataHolder> dataList) {
        this.dataList = dataList;
    }
}
