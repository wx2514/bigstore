package com.wuqing.client.bigstore.bean;

import com.wuqing.client.bigstore.config.Constants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/3/15.
 */
public class DataPack implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据快目录
     */
    private String dirName;

    /**
     * 关系 -1:不相关DP, 0:可疑DP, 1:强相关DP
     */
    private int relation;

    /**
     * 当前处理列
     */
    private String col;

    /**
     * 该DP对于的强相关列
     */
    private List<String> strongRelCols;

    public DataPack(String dirName) {
        this.dirName = dirName;
    }

    public DataPack(String dirName, int relation) {
        this.dirName = dirName;
        this.relation = relation;
    }

    public String getDirName() {
        return dirName;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }

    public int getRelation() {
        return relation;
    }

    public void setRelation(int relation) {
        this.relation = relation;
    }

    public String getCol() {
        return col;
    }

    public List<String> getStrongRelCols() {
        return strongRelCols;
    }

    public void addAllStrongRelCols(List<String> strongRelCols) {
        if (strongRelCols == null) {
            return;
        }
        if (this.strongRelCols == null) {
            this.strongRelCols = new ArrayList<String>();
        }
        this.strongRelCols.addAll(strongRelCols);
    }

    public void addStrongRelCol(String strongRelCol) {
        if (strongRelCol == null) {
            return;
        }
        if (this.strongRelCols == null) {
            this.strongRelCols = new ArrayList<String>();
        }
        this.strongRelCols.add(strongRelCol);
    }

    public DataPack setCol(String col) {
        this.col = col;
        if (this.relation == Constants.RELATION_TAG_ALL) {  //如果是强相关的
            if (this.strongRelCols == null) {
                this.strongRelCols = new ArrayList<String>();
            }
            this.strongRelCols.add(col);
        }
        return this;
    }

    public boolean isStrongRelCol(String col) {
        if (this.strongRelCols == null) {
            return false;
        }
        return this.strongRelCols.contains(col);
    }

    @Override
    public String toString() {
        return "DataPack{" +
                "dirName='" + dirName + '\'' +
                ", relation=" + relation +
                ", col='" + col + '\'' +
                ", strongRelCols=" + strongRelCols +
                '}';
    }
}
