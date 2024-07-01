package com.wuqing.business.bigstore.util;

/**
 * Created by wuqing on 16/6/3.
 * 10进制与64进制转换工具
 */
public class Convert10To64 {

    final static char[] DIGITS = {
            48, 49, 50, 51, 52, 53,
            54, 55, 56, 57, 58, 59,
            60, 61, 62, 63, 64, 65,
            66, 67, 68, 69, 70, 71,
            72, 73, 74, 75, 76, 77,
            78, 79, 80, 81, 82, 83,
            84, 85, 86, 87, 88, 89,
            90, 91, 92, 93, 94, 95,
            96, 97, 98, 99, 100, 101,
            102, 103, 104, 105, 106, 107,
            108, 109, 110, 111,
    };

    /**
     * 把10进制的数字转换成64进制
     *
     * @param number
     * @return
     */
    public static String compressNumber(long number) {
        boolean negative = false;
        if (number < 0) {
            negative = true;
            number = Math.abs(number);
        }
        char[] buf = new char[64];
        int charPos = 64;
        int radix = 1 << 6;
        long mask = radix - 1;
        do {
            buf[--charPos] = DIGITS[(int) (number & mask)];
            number >>>= 6;
        } while (number != 0);
        String res = new String(buf, charPos, (64 - charPos));
        if (negative) {
            res = "-" + res;
        }
        return res;
    }

    /**
     * 把64进制的字符串转换成10进制
     *
     * @param decompStr
     * @return
     */
    private static long unCompressNumber(String decompStr) {
        boolean negative = false;
        if (decompStr.startsWith("-")) {
            decompStr = decompStr.substring(1);
            negative = true;
        }
        long result = 0;
        for (int i = decompStr.length() - 1; i >= 0; i--) {
            if (i == decompStr.length() - 1) {
                result += getCharIndexNum(decompStr.charAt(i));
                continue;
            }
            for (int j = 0; j < DIGITS.length; j++) {
                if (decompStr.charAt(i) == DIGITS[j]) {
                    result += ((long) j) << 6 * (decompStr.length() - 1 - i);
                }
            }
        }
        if (negative) {
            result = - result;
        }
        return result;
    }

    public static long unCompressNumberByLine(String line) {
        line = line.replace("\0", "");
        return unCompressNumber(line);
    }

    /**
     * @param ch
     * @return
     */
    private static long getCharIndexNum(char ch) {
        int num = ((int) ch);
        return num - 48;
        /*if (num >= 48 && num <= 57) {
            return num - 48;
        } else if (num >= 97 && num <= 122) {
            return num - 87;
        } else if (num >= 65 && num <= 90) {
            return num - 29;
        } else if (num == 43) {
            return 62;
        } else if (num == 47) {
            return 63;
        }
        return 0;*/
    }

    /**
     * 获取当前时间的微秒（64进制）
     * @return
     */
    public static String getMicrosecondFor64() {
        long now = System.nanoTime() / 1000;    //微妙
        return compressNumber(now);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        //System.out.println(compressNumber(10));
        System.out.println(unCompressNumber("65B"));
        //System.out.println(unCompressNumber("`"));
        /*for (long i = 1; i <= 20; i++) {
            long v = (long) CommonUtil.pow10(i);
            long l = v - 1;
            //System.out.println("map10t64.put(" + String.valueOf(l).length() + ", " + compressNumber(l).length() + ");");
            System.out.println("map64t10.put(" + compressNumber(l).length() + ", " + String.valueOf(l).length() + ");");
        }*/


        /*for (int i = 0; i < 1; i++) {
            //long now = System.nanoTime() / 1000;    //微妙
            long now = - 1000;    //微妙
            System.out.println(now);
            String now64 = compressNumber(now);
            System.out.println(now64);
            long end = unCompressNumber(now64);
            System.out.println(end);

            long now2 = - 1001;    //微妙
            System.out.println(now2);
            String now642 = compressNumber(now2);
            System.out.println(now642);
            long end2 = unCompressNumber(now642);
            System.out.println(end2);
            System.out.println(String.valueOf(now).compareTo(String.valueOf(now2)));
            System.out.println(now64.compareTo(now642));
        }*/

    }

}
