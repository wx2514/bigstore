package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.config.Constants;

public class HostConfig {

    public HostConfig(String host) {
        this.host = host;
    }

    public HostConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 目标ip
     */
    private String host;

    /**
     * 端口
     */
    private int port = Constants.PORT;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
