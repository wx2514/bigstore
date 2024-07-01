package com.wuqing.business.bigstore.config;

/**
 * Created by wuqing on 17/5/2.
 */
public class PressConstants {

    /**
     * 压缩类型
     * 0: gzip压缩
     * 1: bzip2压缩
     * 2: xz压缩
     */
    public final static int COMPRESS_TYPE = Params.getCompressType();

    /**
     * 压缩类型 的 byte 模式
     */
    public final static byte COMPRESS_TYPE_BYTE = String.valueOf(COMPRESS_TYPE).getBytes()[0];

    /**
     * GZIP压缩
     */
    public final static int GZIP_TYPE = 2;

    /**
     * BZIP压缩
     */
    public final static int BZIP2_TYPE = 3;

    /**
     * XZ压缩
     */
    public final static int XZ_TYPE = 1;

    /**
     * XZ压缩
     */
    public final static int NO_PRESS_TYPE = -1;

    /**
     * bzip压缩文件
     */
    public final static String BZ2_FILE_TYPE = ".bz";

    /**
     * xz压缩文件
     */
    public final static String XZ_FILE_TYPE = ".xz";

    /**
     * 文本文件后缀
     */
    public final static String TXT_TYPE = ".txt";

    /**
     * 压缩文件后缀
     */
    public final static String GZ_TYPE = ".gz";

}
