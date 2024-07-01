package com.fenci;

import com.wuqing.business.bigstore.util.FileUtil;

import java.util.*;
import java.util.StringTokenizer;

/**
 * Created by wuqing on 18/5/21.
 */
public class FenCi {

    public static void main(String[] args) throws Exception {
        List<String> list = FileUtil.readAll("/Users/wuqing/beetle-app.log", false);
        long s = System.currentTimeMillis();
        for (int j = 0; j < 1; j++) {


            Map<String, StringBuilder> map = new HashMap<String, StringBuilder>();
            for (int i = 0, k = list.size(); i < k; i++) {
                StringTokenizer st = new StringTokenizer(list.get(i), "\\\t\n\r\f\"\' ,.:()[]{}<>?~!@#$%^&*-_+=");
                while(st.hasMoreElements()){
                    String token = st.nextToken();
                    if (token.length() > 30) {
                        continue;
                    }
                    StringBuilder sb = map.get(token);
                    if (sb == null) {
                        sb = new StringBuilder();
                        map.put(token, sb);
                    }
                    sb.append(i + "#");
                }
            }
            List<String> ls = new ArrayList<String>();
            for (Map.Entry<String, StringBuilder> entry : map.entrySet()) {
                ls.add(entry.getKey() + ":" + entry.getValue());
            }
            FileUtil.writeFile("/Users/wuqing/beetle-app.log_sp", ls, false);
        }
        System.out.println(System.currentTimeMillis() - s);
    }

}
