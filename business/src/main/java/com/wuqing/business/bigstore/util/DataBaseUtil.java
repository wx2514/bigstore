package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.LongLengthMapping;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;

/**
 * Created by wuqing on 17/3/22.
 */
public class DataBaseUtil {

    private final static Logger logger = LoggerFactory.getLogger(DataBaseUtil.class);

    /**
     * 数据列format
     * @return
     */
    public static String formatData(Object o, int length, boolean isNumber) {
        if (length <= 0) {
            if (isNumber && ServerConstants.USE_64) {
                return Convert10To64.compressNumber((Long) o);
            } else {
                return String.valueOf(o);
            }
        }
        if (isNumber) {
            if (ServerConstants.USE_64) {
                String data = Convert10To64.compressNumber((Long) o);
                StringBuilder sb = new StringBuilder();
                length = LongLengthMapping.get64By10(length);
                for (int i = data.length(); i < length; i++) {
                    sb.append('\0');
                }
                if (data.startsWith("-")) {
                    data = data.substring(0, 1) + sb.toString() + data.substring(1);
                } else {
                    data = sb.toString() + data;
                }
                if (data.length() > length) {
                    logger.warn("data is too long, data:" + data + ", length:" + length);
                    data = data.substring(0, length);
                }
                return data;
            } else {
                DecimalFormat format = getDataFormat(length - 1);
                if (format == null) {
                    return String.valueOf(o);
                }
                String data = format.format(o);  //留一位作为 符号位
                if (data.length() < length) {
                    data = "0" + data;  //正数会少一位补0就可以了，负数正好。
                } else if (data.length() > length) {
                    logger.warn("data is too long, data:" + data + ", length:" + length);
                    data = data.substring(0, length);
                }
                return data;

            }

        } else {
            StringBuilder sb = new StringBuilder();
            String s = String.valueOf(o);
            int zeroCount = length - s.length();
            for (int i = 0; i < zeroCount; i++) {
                sb.append('\0');
            }
            sb.append(s);
            if (sb.length() > length) {
                logger.warn("data is too long, data:" + sb + ", length:" + length);
                return sb.substring(0, length);
            }
            return sb.toString();
        }
    }

    /**
     * 获取表路径
     * @param dataBase
     * @param table
     * @return
     */
    public static String getTablePath(String dataBase, String table) {
        return Params.getBaseDir() + dataBase + "/" + table;
    }

    /**
     * 获取快内行号的 format
     * @return
     */
    public static DecimalFormat getLineNumFormat() {
        int length = String.valueOf(ServerConstants.PARK_SIZ).length() - 1;
        String format = "";
        for (int i = 0; i < length; i++) {
            format += "0";
        }
        return new DecimalFormat(format);
    }

    /**
     * 根据长度获取对应的 format
     * @param length
     * @return
     */
    public static DecimalFormat getDataFormat(int length) {
        DecimalFormat df = null;
        String ftStr = "";
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                ftStr += "0";
            }
            df = new DecimalFormat(ftStr);
        }
        return df;
    }

    /**
     * 进行对比，以字符串形式对比，或者确定为正数对比，不考虑负数
     * @param b1
     * @param b2
     * @return
     */
    public static int compare(byte[] b1, byte[] b2) {
        int l = b1.length - b2.length;
        if (l != 0) {
            return l;
        } else {
            for (int i = 0, k = b1.length; i < k ; i++) {
                int a = b1[i] - b2[i];
                if (a != 0) {
                    return a;
                }
            }
        }
        return 0;   //相等
    }

    /**
     * 进行对比
     * @param b1
     * @param b2
     * @param isNumber  是否为数字对比
     * @return
     */
    public static int compare(byte[] b1, byte[] b2, boolean isNumber) {
        if (b1.length == 0 && b2.length == 0) {
            return 0;
        } else if (b1.length == 0) {
            return -1;
        } else if (b2.length == 0) {
            return 1;
        }
        if (isNumber) {
            if (b1[0] == '-' && b2[0] != '-') {
                return -1;
            } else if (b1[0] != '-' && b2[0] == '-') {
                return 1;
            } else if (b1[0] == '-' && b2[0] == '-') {    //都是负数，取反
                return -compare(b1, b2);
            } else {    //都是正数
                return compare(b1, b2);
            }
        } else {
            return compare(b1, b2);
        }
    }

    public static void main(String[] args) {
        String s = formatData(-1234567L, 5, true);
        System.out.println(s);
        s = formatData("1234567", 5, false);
        System.out.println(s);
    }
}
