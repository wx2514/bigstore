package com.wuqing.business.bigstore.util;

import com.wuqing.client.bigstore.config.Constants;

import java.util.*;

/**
 * 根据 BoyerMoore算法匹配字符
 * Created by wuqing on 18/5/11.
 */
public class BoyerMoore {

    /**
     * 检索字符
     * @param search   模糊查询的字段
     * @param text  被查询的文本
     * @return
     */
    public static List<Integer> match(byte[] search, byte[] text) {
        List<Integer> matches = new ArrayList<Integer>();
        int m = text.length;
        int n = search.length;
        Map<Byte, Integer> rightMostIndexes = preprocessForBadCharacterShift(search);
        int alignedAt = 0;
        while (alignedAt + (n - 1) < m) {
            for (int indexInPattern = n - 1; indexInPattern >= 0; indexInPattern--) {
                int indexInText = alignedAt + indexInPattern;
                byte x = text[indexInText];
                byte y = search[indexInPattern];
                if (indexInText >= m) {
                    break;
                }
                if (x != y) {
                    Integer r = rightMostIndexes.get(x);
                    if (r == null) {
                        alignedAt = indexInText + 1;
                    } else {
                        int shift = indexInText - (alignedAt + r);
                        alignedAt += shift > 0 ? shift : 1;
                    }
                    break;
                } else if (indexInPattern == 0) {
                    matches.add(alignedAt);
                    alignedAt++;
                }
            }
        }
        return matches;
    }

    /*public static List<Integer> match(String pattern, String text) {
        List<Integer> matches = new ArrayList<Integer>();
        int m = text.length();
        int n = pattern.length();
        Map<Character, Integer> rightMostIndexes = preprocessForBadCharacterShift(pattern);
        int alignedAt = 0;
        while (alignedAt + (n - 1) < m) {
            for (int indexInPattern = n - 1; indexInPattern >= 0; indexInPattern--) {
                int indexInText = alignedAt + indexInPattern;
                char x = text.charAt(indexInText);
                char y = pattern.charAt(indexInPattern);
                if (indexInText >= m) break;
                if (x != y) {
                    Integer r = rightMostIndexes.get(x);
                    if (r == null) {
                        alignedAt = indexInText + 1;
                    } else {
                        int shift = indexInText - (alignedAt + r);
                        alignedAt += shift > 0 ? shift : 1;
                    }
                    break;
                } else if (indexInPattern == 0) {
                    matches.add(alignedAt);
                    alignedAt++;
                }
            }
        }
        return matches;
    }*/

    private static Map<Byte, Integer> preprocessForBadCharacterShift(byte[] pattern) {
        Map<Byte, Integer> map = new HashMap<Byte, Integer>();
        for (int i = pattern.length - 1; i >= 0; i--) {
            byte c = pattern[i];
            if (!map.containsKey(c)) {
                map.put(c, i);
            }
        }
        return map;
    }

    private static Map<Character, Integer> preprocessForBadCharacterShift(String pattern) {
        Map<Character, Integer> map = new HashMap<Character, Integer>();
        for (int i = pattern.length() - 1; i >= 0; i--) {
            char c = pattern.charAt(i);
            if (!map.containsKey(c)) {
                map.put(c, i);
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        //List<Integer> matches = match("ana", "bananas");
        byte[] text = GZipUtil.read2Byte("/Users/wuqing/Downloads/apollo_request.txt");
        byte[] pattern = "04903678D490110A5943363B0006FB86".getBytes(Constants.DEFAULT_CHARSET);
        long start = System.currentTimeMillis();
        List<Integer> matches = match(pattern, text);
        System.out.println("time:" + (System.currentTimeMillis() - start));
        int idxStart = matches.get(0);
        byte[] res = Arrays.copyOfRange(text, idxStart, idxStart + pattern.length);
        System.out.println(new String(res, Constants.DEFAULT_CHARSET));
        /*System.out.println("start to match");
        for (int i = 0; i < 1; i++) {
            long start = System.currentTimeMillis();
            List<Integer> matches = match("艾美衣服本宫", sb.toString());
            //System.out.println(matches.get(0));
            //System.out.println(sb.toString().indexOf("艾美衣服本宫"));
            System.out.println("time:" + (System.currentTimeMillis() - start));
        }
        System.out.println("end to match");

        String sss = "艾美衣服本宫";
        System.out.println(sss.indexOf("衣服"));*/

        /*for (Integer integer : matches) {
            System.out.println("Match at: " + integer);
        }*/
        /*int index = str.indexOf("getBaseUserInfo");
        System.out.println(index);
        int lastIndex = str.lastIndexOf("getBaseUserInfo");
        System.out.println(lastIndex);*/
    }
}
