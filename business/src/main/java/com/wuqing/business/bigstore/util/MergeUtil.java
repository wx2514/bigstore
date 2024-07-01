package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.util.CommonUtil;

import java.util.*;

/**
 * 区间合并算法
 *
 * @author wangxu
 */
public class MergeUtil {

    public static List<IndexInfo.Index> merge(List<IndexInfo.Index> intervalList) {
        List<IndexInfo.Index> res = new ArrayList<>();
        if (CommonUtil.isEmpty(intervalList)) {
            return res;
        }
        Collections.sort(intervalList, new Comparator<IndexInfo.Index>() {
            @Override
            public int compare(IndexInfo.Index o1, IndexInfo.Index o2) {
                long l = o1.getStart() - o2.getStart();
                if (l < 0) {
                    return -1;
                } else if (l > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        //定义第一个合并后的区间开始
        long start = intervalList.get(0).getStart();
        //定义第一个合并后的区间结束
        long end = intervalList.get(0).getEnd();
        for (IndexInfo.Index i : intervalList) {

            //如果当前遍历到的区间start小于合并后的区间end，说明当前区间和合并后的区间存在重合
            if (i.getStart() <= end) {
                //需要把重合的区间合并到合并后的区间中
                end = Math.max(end, i.getEnd());
            } else {
                //else说明当前和要合并的区间没有重合,把合并后的区间加入到res中，并重置合并后的区间start和end
                res.add(new IndexInfo.Index(start, end));
                start = i.getStart();
                end = i.getEnd();
            }
        }
        res.add(new IndexInfo.Index(start, end));
        return res;
    }

    /**
     * 把过大的分段索引，进行合并，合并成较小的
     * @param mergeIndex
     * @return
     */
    public static List<IndexInfo.Index> merge2small(List<IndexInfo.Index> mergeIndex) {
        /*int bei = mergeIndex.size() / ServerConstants.PARK_INDEX_SIZE;
        if (bei <= 1) {
            return mergeIndex;
        }*/
        List<IndexInfo.Index> result = new ArrayList<>(ServerConstants.PARK_INDEX_SIZE);
        IndexInfo.Index last = null;
        for (int i = 0, k = mergeIndex.size(); i < k; i++) {
            IndexInfo.Index now = mergeIndex.get(i);
            if (last == null) {
                last = now;
            } else {
                //如果是基本连续的，没有大的跳跃则直接合并
                if ((float) now.getStart() / last.getEnd() < 1.2) {
                    last.setEnd(now.getEnd());
                } else {
                    result.add(last);
                    last = now;
                }
            }
            /*if ((i + 1) % bei == 0) {
                result.add(last);
                last = null;
            }*/
        }
        if (last != null) {
            result.add(last);
        }
        return result;
    }

    public static void main(String[] args) {
        /*List<IndexInfo.Index> intervals = new ArrayList<>();
        IndexInfo.Index interval = new IndexInfo.Index(1, 3);
        intervals.add(interval);
        interval = new IndexInfo.Index(20, 22);
        intervals.add(interval);
        interval = new IndexInfo.Index(2, 4);
        intervals.add(interval);
        interval = new IndexInfo.Index(10, 20);
        intervals.add(interval);
        interval = new IndexInfo.Index(5, 7);
        intervals.add(interval);


        List<IndexInfo.Index> res = merge(intervals);
        res.forEach(p -> {
            System.out.println(p.getStart() + ":" + p.getEnd());
        });*/

        List<IndexInfo.Index> mergeIndex = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 50000;) {
            IndexInfo.Index id = new IndexInfo.Index(i, i + random.nextInt(6));
            mergeIndex.add(id);
            i += 5;
        }
        System.out.println(mergeIndex.get(0).getStart() + ":" + mergeIndex.get(mergeIndex.size() - 1).getEnd() + ":" + mergeIndex.size());
        mergeIndex = MergeUtil.merge2small(mergeIndex);
        System.out.println(mergeIndex.get(0).getStart() + ":" + mergeIndex.get(mergeIndex.size() - 1).getEnd() + ":" + mergeIndex.size());

    }

}
