package com.other;

import com.wuqing.business.bigstore.util.PoolUtil;

/**
 * Created by wuqing on 17/8/14.
 */
public class SemTest {

    public static void main(String[] args) throws InterruptedException {
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.acquire();
        PoolUtil.COMPRESS_SEMAP.release();
        PoolUtil.COMPRESS_SEMAP.release();
        PoolUtil.COMPRESS_SEMAP.release();
        PoolUtil.COMPRESS_SEMAP.release();
    }

}
