package com.other;

import com.wuqing.business.bigstore.util.FileUtil;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wuqing on 17/8/14.
 */
public class AnalyseLog {

    public static void main(String[] args) throws Exception {
        Map<String, Long> map = new HashMap<String, Long>();
        List<String> list = FileUtil.readAll("/Users/wuqing/sss.log", false);
        for (String s : list) {
            String[] array = StringUtils.splitPreserveAllTokens(s, "\\|");
            String time = array[0];
            long count = Long.parseLong(array[10]);
            int type = Integer.parseInt(array[1]);
            if (type != 6) {
                continue;
            }
            Long l = map.get(time);
            if ( l == null) {
                map.put(time, count);
            } else {
                map.put(time, count + l);
            }
        }
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

}
