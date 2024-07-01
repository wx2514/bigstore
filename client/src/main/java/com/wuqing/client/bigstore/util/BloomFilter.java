package com.wuqing.client.bigstore.util;

import com.wuqing.client.bigstore.bean.BitSet;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 布隆过滤器
 * 警告：这个对象中的元素不能发生改变，否则序列化，反序列化 会报错 导致故障
 */
public class BloomFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int[] seeds;
    private final int size;
    private final BitSet notebook;
    private final MisjudgmentRate rate;
    private final AtomicInteger useCount = new AtomicInteger();
    private final Double autoClearRate;

    /**
     *
     * @param dataCount 预期处理的数据规模
     * @param rate  区分度
     */
    public BloomFilter(int dataCount, MisjudgmentRate rate) {
        this(rate, dataCount, null);
    }


    /**
     * 自动清空过滤器内部信息的使用比率，传null则表示不会自动清理;
     * 当过滤器使用率达到100%时，则无论传入什么数据，都会认为在数据已经存在了;
     * 当希望过滤器使用率达到80%时自动清空重新使用，则传入0.8
     *
     * @param rate
     * @param dataCount
     * @param autoClearRate
     */
    public BloomFilter(MisjudgmentRate rate, int dataCount, Double autoClearRate) {
        //每个字符串需要的bit位数*总数据量
        long bitSize = rate.seeds.length * dataCount;
        if (bitSize < 0 || bitSize > Integer.MAX_VALUE) {
            throw new RuntimeException("位数太大溢出了，请降低误判率或者降低数据大小");
        }
        this.rate = rate;
        seeds = rate.seeds;
        size = (int) bitSize;
        //创建一个BitSet位集合
        notebook = new BitSet(size);
        this.autoClearRate = autoClearRate;
    }

    //如果存在返回true,不存在返回false
    public boolean addIfNotExist(String data) {
        //是否需要清理
        checkNeedClear();
        //seeds.length决定每一个string对应多少个bit位，每一位都有一个索引值
        //给定data，求出data字符串的第一个索引值index，如果第一个index值对应的bit=false说明，该data值不存在，则直接将所有对应bit位置为true即可;
        //如果第一个index值对应bit=true，则将index值保存，但此时并不能说明data已经存在，
        //则继续求解第二个index值，若所有index值都不存在则说明该data值不存在，将之前保存的index数组对应的bit位置为true
        int[] indexs = new int[seeds.length];
        //假定data已经存在
        boolean exist = true;
        int index;
        for (int i = 0; i < seeds.length; i++) {
            //计算位hash值
            indexs[i] = index = hash(data, seeds[i]);
            if (exist) {
                //如果某一位bit不存在，则说明该data不存在
                if (!notebook.get(index)) {
                    exist = false;
                    //将之前的bit位置为true
                    for (int j = 0; j <= i; j++) {
                        setTrue(indexs[j]);
                    }
                }
            } else {
                //如果不存在则直接置为true
                setTrue(index);
            }
        }

        return exist;
    }

    public boolean exist(String data) {
        //是否需要清理
        checkNeedClear();
        //seeds.length决定每一个string对应多少个bit位，每一位都有一个索引值
        //给定data，求出data字符串的第一个索引值index，如果第一个index值对应的bit=false说明，该data值不存在，则直接将所有对应bit位置为true即可;
        //如果第一个index值对应bit=true，则将index值保存，但此时并不能说明data已经存在，
        //则继续求解第二个index值，若所有index值都不存在则说明该data值不存在，将之前保存的index数组对应的bit位置为true
        int[] indexs = new int[seeds.length];
        //假定data已经存在
        boolean exist = true;
        int index;
        for (int i = 0; i < seeds.length; i++) {
            //计算位hash值
            indexs[i] = index = hash(data, seeds[i]);
            if (exist) {
                //如果某一位bit不存在，则说明该data不存在
                if (!notebook.get(index)) {
                    exist = false;
                }
            }
        }

        return exist;
    }

    private int hash(String data, int seeds) {
        char[] value = data.toCharArray();
        int hash = 0;
        if (value.length > 0) {
            for (int i = 0; i < value.length; i++) {
                hash = i * hash + value[i];
            }
        }
        hash = hash * seeds % size;
        return Math.abs(hash);
    }

    private void setTrue(int index) {
        useCount.incrementAndGet();
        notebook.set(index, true);
    }

    //如果BitSet使用比率超过阈值，则将BitSet清零
    private void checkNeedClear() {
        if (autoClearRate != null) {
            if (getUseRate() >= autoClearRate) {
                synchronized (this) {
                    if (getUseRate() >= autoClearRate) {
                        notebook.clear();
                        useCount.set(0);
                    }
                }
            }
        }
    }

    private Double getUseRate() {
        return (double) useCount.intValue() / (double) size;
    }

    /**
     * 清空过滤器中的记录信息
     */
    public void clear() {
        useCount.set(0);
        notebook.clear();
    }

    public MisjudgmentRate getRate() {
        return rate;
    }



    public static void main(String[] args) {
        /*BloomFilter fileter = new BloomFilter(7);
        System.out.println(fileter.addIfNotExist("1111111111111"));
        System.out.println(fileter.addIfNotExist("2222222222222222"));
        System.out.println(fileter.addIfNotExist("3333333333333333"));
        System.out.println(fileter.addIfNotExist("444444444444444"));
        System.out.println(fileter.addIfNotExist("5555555555555"));
        System.out.println(fileter.addIfNotExist("6666666666666"));
        System.out.println(fileter.addIfNotExist("1111111111111"));
        System.out.println(fileter.getUseRate());
        System.out.println(fileter.addIfNotExist("1111111111111"));*/
        int size = 1000000;
        BloomFilter fileter = new BloomFilter(size, MisjudgmentRate.HIGH);
        int j = 0;
        //String traceId = "sky:TID:de85325cf4e343c1a71ed05b16799fae.526083.16396494000260001;";
        String traceId = "234959291";
        int count = 0;
        for (int i = 0; i < size; i++) {
            boolean res = fileter.addIfNotExist(traceId + j++);
            //应该都是false

            if (res) {
                count++;
                //System.out.println("WARN:" + true);
            }
        }
        System.out.println("===percent: " + (float) count / size * 100 + "%");

        count = 0;
        j = 10000000;
        for (int i = 0; i < size; i++) {
            boolean res = fileter.exist(traceId + j++);
            //应该都是 false

            if (res) {
                count++;
                //System.out.println("ERROR:" + true);
            }
        }
        System.out.println("===percent2: " + (float) count / size * 100 + "%");

        count = 0;
        j = 0;
        for (int i = 0; i < size; i++) {
            boolean res = fileter.exist(traceId + j++);
            //应该都是 true

            if (!res) {
                System.out.println("ERROR:" + res);
            }
        }

        byte[] bytes = KryoUtil.writeToByteArray(fileter);
        System.out.println("length:" + bytes.length);
        BloomFilter f = (BloomFilter) KryoUtil.readFromByteArray(bytes);
        System.out.println(f.exist("sky:TID:de85325cf4e343c1a71ed05b16799fae.526083.16396494000260001;1000"));
        System.out.println(f.exist("sky:TID:de85325cf4e343c1a71ed05b16799fae.526083.16396494000260001;7"));
        bytes = KryoUtil.writeToByteArray(fileter);
        f = (BloomFilter) KryoUtil.readFromByteArray(bytes);
        System.out.println(f.exist("sky:TID:de85325cf4e343c1a71ed05b16799fae.526083.16396494000260001;1000001"));
    }


}
