package com.wuqing.client.bigstore.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wuqing on 17/4/3.
 */
public class EnumInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SPLIT = "\t";

    private List<String> line = null;

    Map<String, Long> enumMap = new ConcurrentHashMap<String, Long>();

    public EnumInfo() {
        line = new ArrayList<String>();
    }

    /**
     * 解析文本信息
     * @param line
     */
    public EnumInfo(List<String> line) {
        if (line == null || line.isEmpty()) {
            line = new ArrayList<String>();
            return;
        }
        this.line = line;
        for (int i = 0, k = line.size(); i < k; i++) {
            enumMap.put(line.get(i), (long) i);
        }
    }

    /**
     * 添加枚举类型
     * @param em
     * @return
     */
    public long addEnum(String em) {
        this.line.add(em);
        long index = (this.line.size() - 1);
        enumMap.put(em, index);
        return index;
    }

    /**
     * 根据内容获取索引
     * @param em
     * @return
     */
    public Long getIndex(String em) {
        enumMap.get(em);
        return enumMap.get(em);
    }

    public String getData(Long index) {
        if (index == null) {
            return null;
        }
        if (line.size() <= index) {
            return null;
        }
        return line.get(index.intValue());
    }

    public int size() {
        return this.line.size();
    }

    /**
     * 转化成文件格式
     * @return
     */
    public List<String> toTextLines() {
        return line;
    }
}
