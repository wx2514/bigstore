package com.wuqing.ui.bigstore.start;

import com.wuqing.ui.bigstore.server.UiServer;

public class UiStart {

    public static void main(String[] args) {
        try {
            UiServer uiServer = new UiServer(80);
            uiServer.startServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
