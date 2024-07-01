package com.wuqing.test.memory;

/**
 * Created by wuqing on 16/6/3.
 * 10进制与64进制转换工具
 */
public class Convert10To64 {

    private final static int ADVANCE = 128;

    private final static int MOVE = 7;

    final static char[] digits = {
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
            112, 113, 114, 115, 116, 117,
            118, 119, 120, 121, 122, 123,
            124, 125, 126, 127, 128, 129,
            130, 131, 132, 133, 134, 135,
            136, 137, 138, 139, 140, 141,
            142, 143, 144, 145, 146, 147,
            148, 149, 150, 151, 152, 153,
            154, 155, 156, 157, 158, 159,
            160, 161, 162, 163, 164, 165,
            166, 167, 168, 169, 170, 171,
            172, 173, 174, 175,
    };

    /**
     * 把10进制的数字转换成64进制
     *
     * @param number
     * @return
     */
    public static char[] compressNumber(long number) {
        boolean negative = false;
        if (number < 0) {
            negative = true;
            number = Math.abs(number);
        }
        char[] buf = new char[ADVANCE];
        int charPos = ADVANCE;
        int radix = 1 << MOVE;
        long mask = radix - 1;
        do {
            buf[--charPos] = digits[(int) (number & mask)];
            number >>>= MOVE;
        } while (number != 0);
        int length = ADVANCE - charPos;
        char[] res = null;
        if (negative) {
            res = new char[length + 1];
            res[0] = '-';
            System.arraycopy(buf, charPos, res, 1, length);
        } else {
            res = new char[length];
            System.arraycopy(buf, charPos, res, 0, length);
        }
        return res;
    }

    /**
     * 把64进制的字符串转换成10进制
     *
     * @param decomp
     * @return
     */
    private static long unCompressNumber(char[] decomp) {
        if (decomp == null || decomp.length == 0) {
            return 0L;
        }
        boolean negative = false;
        int end = 0;
        if (decomp[0] == '-') {
            negative = true;
            end = 1;
        }
        long result = 0;
        for (int i = decomp.length - 1; i >= end; i--) {
            if (i == decomp.length - 1) {
                result += getCharIndexNum(decomp[i]);
                continue;
            }
            for (int j = 0; j < digits.length; j++) {
                if (decomp[i] == digits[j]) {
                    result += ((long) j) << MOVE * (decomp.length - 1 - i);
                }
            }
        }
        if (negative) {
            result = - result;
        }
        return result;
    }

    /*public static long unCompressNumberByLine(String line) {
        line = line.replace("\0", "");
        return unCompressNumber(line);
    }*/

    /**
     * @param ch
     * @return
     */
    private static long getCharIndexNum(char ch) {
        int num = ch;
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
     * @param args
     */
    public static void main(String[] args) {
        /*char c = 200;
        byte b = (byte) c;
        char c2 = 201;
        byte b2 = (byte) c2;*/
        char[] bs = compressNumber(-64636363L);
        System.out.println(unCompressNumber(bs));
        //System.out.println(unCompressNumber("`"));
        /*for (long i = 1; i <= 20; i++) {
            long v = (long) CommonUtil.pow(i);
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
