package com.wuqing.business.bigstore.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 16/9/20.
 */
public class StreamGobbler extends Thread {
    private InputStream is;
    private String type;  //输出流的类型ERROR或OUTPUT
    private List<String> lineList = new ArrayList<String>();
    private boolean complete = false;

    public StreamGobbler(InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    @Override
    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                lineList.add(line);
                /*System.out.println(type + ">" + line);
                System.out.flush();*/
            }
            complete = true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public List<String> getLineList() {
        return lineList;
    }

    public boolean isComplete() {
        return complete;
    }
}
