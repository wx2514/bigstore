package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.bean.StringDataLine;

import java.util.*;

/**
 * Created by wuqing on 18/5/21.
 */
public class SplitBuilder {

    private static final String SPLIT_KEY = "\\\t\n\r\f\"\' ,.:;()[]{}<>?~!@#$%^&*-_+=/";

    public static final char[] SPLIT_KEY_CHARS = SPLIT_KEY.toCharArray();

    /**
     * <分组, <关键字, 统计文件>>
     */
    private Map<Integer, Map<String, StringBuilderLast>> spMap = new HashMap<Integer, Map<String, StringBuilderLast>>();

    public Set<String> add(StringDataLine dataLine) {
        String line = dataLine.getData();
        StringTokenizer st = new StringTokenizer(line, SPLIT_KEY);
        Set<String> set = new HashSet<String>();    //用于去重复
        while(st.hasMoreElements()){
            String token = st.nextToken();
            int length = token.length();
            if (length <=1 || length > 20) {  //超过20的不记录,1长度的也不记录
                continue;
            }
            if (!set.add(token)) {
                continue;
            }
            int group = Math.abs(token.hashCode()) % ServerConstants.GROUP_SIZE;
            Map<String, StringBuilderLast> map = spMap.get(group);
            if (map == null) {
                map = new HashMap<String, StringBuilderLast>();
                spMap.put(group, map);
            }
            StringBuilderLast sb = map.get(token);
            if (sb == null) {
                sb = new StringBuilderLast(token + ServerConstants.INDEX_FLAG);
                map.put(token, sb);
            }
            sb.append(dataLine.getNumber());
        }
        return set;
    }

    public Map<Integer, List<String>> toLines() {
        Map<Integer, List<String>> result = new HashMap<Integer, List<String>>();
        for (Map.Entry<Integer, Map<String, StringBuilderLast>> entry : spMap.entrySet()) {
            int group = entry.getKey();
            List<String> lines = result.get(group);
            if (lines == null) {
                lines = new ArrayList<String>();
                result.put(group, lines);
            }
            for (Map.Entry<String, StringBuilderLast> en : entry.getValue().entrySet()) {
                StringBuilderLast sb = en.getValue();
                lines.add(sb.toString());
            }
        }
        return result;
    }

    static class StringBuilderLast {
        private StringBuilder sb;
        private int last = -1;

        public StringBuilderLast(String key) {
            this.sb = new StringBuilder(key);
        }

        @Override
        public String toString() {
            return this.sb.substring(0, this.sb.length() - 1);
        }

        public void append(int append) {
            if (this.last == -1) {
                this.sb.append((ServerConstants.USE_64 ? Convert10To64.compressNumber(append) : append) + ServerConstants.DESC_LINE_SPLIT);
            } else {
                int record = append - this.last;
                this.sb.append((ServerConstants.USE_64 ? Convert10To64.compressNumber(record) : record) + ServerConstants.DESC_LINE_SPLIT);
            }
            this.last = append;

        }

    }

}
