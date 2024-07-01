package com.wuqing.client.bigstore.bean;

import java.io.Serializable;

/**
 * Created by wuqing on 16/11/17.
 * 文件数据同步的爆
 */
public class FileData implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 文件路径
     */
    private String filePath;

    /**
     * 数据
     */
    private byte[] fileData;

    public FileData(String filePath, byte[] fileData) {
        this.filePath = filePath;
        this.fileData = fileData;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }
}
