package com.wuqing.client.bigstore.util;

/**
 * 分配的位数越多，误判率越低但是越占内存
 * <p>
 * 4个位误判率大概是0.14689159766308
 * <p>
 * 8个位误判率大概是0.02157714146322
 * <p>
 * 16个位误判率大概是0.00046557303372
 * <p>
 * 32个位误判率大概是0.00000021167340
 */
public enum MisjudgmentRate {
    // 这里要选取质数，能很好的降低错误率
    /**
     * 每个字符串分配4个位
     */
    VERY_SMALL(new int[]{2, 3, 5, 7}),

    /**
     * 每个字符串分配6个位
     */
    LITTLE_SMALL(new int[]{2, 3, 5, 7, 11, 13}),

    /**
     * 每个字符串分配8个位
     */
    SMALL(new int[]{2, 3, 5, 7, 11, 13, 17, 19}),

    /**
     * 每个字符串分配16个位
     */
    MIDDLE(new int[]{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53}),

    /**
     * 每个字符串分配32个位
     */
    HIGH(new int[]{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131});

    protected int[] seeds;

    //枚举类型MIDDLE构造函数将seeds数组初始化
    private MisjudgmentRate(int[] seeds) {
        this.seeds = seeds;
    }

    public int[] getSeeds() {
        return seeds;
    }

    public void setSeeds(int[] seeds) {
        this.seeds = seeds;
    }
}
