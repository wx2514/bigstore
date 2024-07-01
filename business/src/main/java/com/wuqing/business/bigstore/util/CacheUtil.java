package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.bean.SpaceInfo;
import com.wuqing.business.bigstore.bean.TableInfo;
import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.cache.TableCache;
import com.wuqing.business.bigstore.config.LongLengthMapping;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.config.PressConstants;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.EnumInfo;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.config.SpaceDescEnum;
import com.wuqing.client.bigstore.util.BloomFilter;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wuqing on 17/3/22.
 */
public class CacheUtil {

    private final static Logger queryTimeLogger = LoggerFactory.getLogger("query-time-log");

    public static byte[] getDpData(String dataBase, String table, String space, String dir, String col) throws Exception {
        byte[] bytes = DataCache.get(dataBase, table, dir, col);

        if (bytes != null) {
            return bytes;
        }
        String key = null;
        //long s = System.currentTimeMillis();
        String spacePath = DataBaseUtil.getTablePath(dataBase, table) + "/rows/" + space;
        File mergerFile = new File(spacePath + "/" + FileConfig.MERGER_FLAG);
        boolean mergerExisits = mergerFile.exists();
        if (mergerExisits) {
            key = Params.getBaseDir() + dataBase + "/" + table + "/rows/" + space + "/" + col;
        } else {
            key = Params.getBaseDir() + dataBase + "/" + table + "/rows/" + space + "/" + dir + "/" + col;
        }
        IndexInfo info = CacheUtil.getIndexInfo(dataBase, table, space, dir, col);
        String dataPath = null;
        if (info != null && info.isPress()) {
            if (info.getPress() == PressConstants.BZIP2_TYPE) {
                dataPath = key + FileConfig.DATA_FILE_BZ2_SUFFIX;
            } if (info.getPress() == PressConstants.XZ_TYPE) {
                dataPath = key + FileConfig.DATA_FILE_XZ_SUFFIX;
            } else {
                dataPath = key + FileConfig.DATA_FILE_GZ_SUFFIX;
            }
        } else {
            dataPath = key + FileConfig.DATA_FILE_SUFFIX;
        }
        ReentrantLock lock = FileLockUtil.getLock(dataPath);;
        lock.lock();
        try {
            //加锁之后再获取一下 试试，避免加锁期间被缓存
            bytes = DataCache.get(dataBase, table, dir, col);
            if (bytes != null) {
                return bytes;
            }
            //后面加文件锁，避免同一文件被多次读取
            //System.out.println("read-file=" + dataPath);
            byte[] dpBytes = GZipUtil.read2Byte(dataPath);
            if (mergerExisits) {
                byte[] uncompress = SnappyUtil.decompressWithCheck(dpBytes);
                if (uncompress != null) {   //如果压缩过进行解压缩
                    dpBytes = uncompress;
                }
                int dirStart = 0;
                int dataStart = 0;
                int dataEnd = 0;
                String dirInMerger = null;
                //long s = System.currentTimeMillis();
                TableInfo tableInfo = CacheUtil.readTableInfo(dataBase, table);

                ColumnDef def = tableInfo.getColumnDef(col);
                int lineLenth = 1;
                if (def.getLength() > 0) {
                    ColumnDef numDef = tableInfo.getColumnDef(Constants.COLUMN_ID);
                    int lineSeg = numDef.getLength() + 1;
                    int numSeg = def.getLength() + 1;
                    if (ServerConstants.USE_64) {
                        lineSeg = LongLengthMapping.get64By10(lineSeg - 1) + 1;
                        if (def.isLong() || def.isEnum()) {
                            numSeg = LongLengthMapping.get64By10(numSeg - 1) + 1;
                        }
                    }
                    lineLenth = lineSeg + numSeg;
                }

                int dirLength = ServerConstants.DIR_LENGTH + 1;
                for (int i = 0, k = dpBytes.length; i < k;) {
                    byte b = dpBytes[i];
                    if (b == ServerConstants.DIR_FLAG_START) {
                        dataEnd = i;
                        if (dataEnd > dataStart) {
                            byte[] sub = Arrays.copyOfRange(dpBytes, dataStart, dataEnd);
                            DataCache.put(dataBase, table, dirInMerger, col, sub);
                            if (dirInMerger != null && dirInMerger.equals(dir)) {
                                bytes = sub;
                            }
                        }
                        dirStart = i + 1;
                        i += dirLength;
                    } else if (b == ServerConstants.DIR_FLAG_END) {
                        dataStart = i + 2;  //还有个换行符
                        i += 2;
                        dirInMerger = new String(dpBytes, dirStart, ServerConstants.DIR_LENGTH);
                    } else {
                        i += lineLenth;
                    }
                }
                //记录最后一个DP信息
                byte[] sub = Arrays.copyOfRange(dpBytes, dataStart, dpBytes.length);
                DataCache.put(dataBase, table, dirInMerger, col, sub);
                if (bytes == null) {    //如果没拿到 则从子文件中拿一次试试，兼容掉未同步的数据
                    key = Params.getBaseDir() + dataBase + "/" + table + "/rows/" + space + "/" + dir + "/" + col;
                    if (info != null && info.isPress()) {
                        if (info.getPress() == PressConstants.BZIP2_TYPE) {
                            dataPath = key + FileConfig.DATA_FILE_BZ2_SUFFIX;
                        } if (info.getPress() == PressConstants.XZ_TYPE) {
                            dataPath = key + FileConfig.DATA_FILE_XZ_SUFFIX;
                        } else {
                            dataPath = key + FileConfig.DATA_FILE_GZ_SUFFIX;
                        }
                    } else {
                        dataPath = key + FileConfig.DATA_FILE_SUFFIX;
                    }
                    bytes = GZipUtil.read2Byte(dataPath);
                    DataCache.put(dataBase, table, dir, col, bytes);
                }
                //System.out.println("split-time:" + (System.currentTimeMillis() - s));
            } else {
                DataCache.put(dataBase, table, dir, col, dpBytes);
                bytes = dpBytes;
            }
        } catch (Exception e) {
            queryTimeLogger.error("read DP file to cache fail", e);
        } finally {
            lock.unlock();
        }
        return bytes;
    }

    /**
     * 获取表分区索引
     * @param dataBase
     * @param table
     * @param dirSpaceIndex
     * @return
     */
    public static SpaceInfo getSpaceInfo(String dataBase, String table, int dirSpaceIndex, String col) throws Exception {
        DecimalFormat df = new DecimalFormat("0000000000");
        String dirSpaceIndexPath = df.format(dirSpaceIndex);
        return  getSpaceInfo(dataBase, table, dirSpaceIndexPath, col);
    }

    /**
     * 获取表分区索引
     * @param dataBase
     * @param table
     * @param space
     * @return
     */
    public static SpaceInfo getSpaceInfo(String dataBase, String table, String space, String col) throws Exception {
        String key = DataBaseUtil.getTablePath(dataBase, table) + "/rows/" + space + "/" + col;
        SpaceInfo cache = TableCache.getSpaceInfo(dataBase, table, space, col);
        if (cache != null) {
            return cache;
        }
        String spacePath = key + FileConfig.SPACE_FILE_SUFFIX;
        SpaceInfo spaceInfo = null;
        ReentrantLock lock = FileLockUtil.getLock(spacePath);
        String bloomSpace =  key + FileConfig.SPACE_BLOOM_SUFFIX;
        lock.lock();
        try {
            cache = TableCache.getSpaceInfo(dataBase, table, space, col);
            if (cache != null) {
                return cache;
            }
            if (new File(spacePath).exists()) {
                List<String> lines = FileUtil.readAll(spacePath, false);
                if (lines == null || lines.size() < SpaceDescEnum.values().length) {
                    return null;
                }
                long min = CommonUtil.parseLong(lines.get(SpaceDescEnum.START.ordinal()));
                long max = CommonUtil.parseLong(lines.get(SpaceDescEnum.END.ordinal()));
                //头2行去掉
                lines.remove(0);
                lines.remove(0);
                spaceInfo = new SpaceInfo(min, max, lines);
            } else if (new File(bloomSpace).exists()) {
                BloomFilter filter = BloomUtil.readBloom(bloomSpace);
                spaceInfo = new SpaceInfo(filter);
            } else {
                return null;
            }
            TableCache.putSpaceInfo(dataBase, table, space, col, spaceInfo);
        } finally {
            lock.unlock();
        }
        return spaceInfo;
    }

    /**
     * 从缓存获取索引数据
     * @param dataBase
     * @param table
     * @param dir
     * @param col
     * @return
     * @throws Exception
     */
    public static IndexInfo getIndexInfo(String dataBase, String table, String dir, String col) throws Exception {
        DecimalFormat df = new DecimalFormat(ServerConstants.DIR_FORMAT);
        int dirSpaceIndex = CommonUtil.parseInt(dir) / Constants.SPACE_SIZ;
        String space = df.format(dirSpaceIndex);
        return getIndexInfo(dataBase, table, space, dir, col);
    }

    /**
     * 从缓存获取索引数据
     * @param dataBase
     * @param table
     * @param space
     * @param dir
     * @param col
     * @return
     * @throws Exception
     */
    public static IndexInfo getIndexInfo(String dataBase, String table, String space, String dir, String col) throws Exception {
        IndexInfo info = TableCache.getIndex(dataBase, table, dir, col);
        if (info != null) {
            return info;
        }
        //long s = System.currentTimeMillis();
        String spacePath = DataBaseUtil.getTablePath(dataBase, table) + "/rows/" + space;
        String spaceDesc = spacePath + "/" + col + FileConfig.DESC_FILE_SUFFIX;
        File mergerFile = new File(spacePath + "/" + FileConfig.MERGER_FLAG);
        if (mergerFile.exists()) {  //如果是已经压缩的，就从压缩的文件中获取
            ReentrantLock lock = FileLockUtil.getLock(spaceDesc);
            lock.lock();
            try {
                info = TableCache.getIndex(dataBase, table, dir, col);
                if (info != null) {
                    return info;
                }
                byte[] indexBytes = GZipUtil.read2Byte(spaceDesc);
                if (indexBytes == null) {
                    return null;
                }
                byte[] unpress = SnappyUtil.decompressWithCheck(indexBytes);
                if (unpress != null) {  //如果是压缩过的，解压缩
                    indexBytes = unpress;
                }
                int dirStart = 0;
                int dataStart = 0;
                int lineStart = 0;
                String dirInMerger = null;
                List<String> lines = new ArrayList<String>();
                for (int i = 0, k = indexBytes.length; i < k; i++) {
                    byte b = indexBytes[i];
                    if (b == ServerConstants.DIR_FLAG_START) {
                        if (lines.size() > 0) {
                            IndexInfo indexInfo = new IndexInfo(lines);
                            TableCache.putIndex(dataBase, table, dirInMerger, col, indexInfo);
                            lines.clear();
                            if (dirInMerger != null && dirInMerger.equals(dir)) {
                                info = indexInfo;
                            }
                        }
                        dirStart = i + 1;
                    } else if (b == ServerConstants.DIR_FLAG_END) {
                        dataStart = i + 2;  //还有个换行符
                        dirInMerger = new String(indexBytes, dirStart, ServerConstants.DIR_LENGTH);
                    } else if (b == ServerConstants.LINE_SEPARATOR) {
                        if (lineStart >= dataStart) {
                            lines.add(new String(indexBytes, lineStart, i - lineStart));
                        }
                        lineStart = i + 1;
                    }
                }
                //记录最后一个index
                if (lines.size() > 0) {
                    IndexInfo indexInfo = new IndexInfo(lines);
                    TableCache.putIndex(dataBase, table, dirInMerger, col, indexInfo);
                    lines.clear();
                    if (dirInMerger != null && dirInMerger.equals(dir)) {
                        info = indexInfo;
                    }
                }
                if (info == null) {
                    String descFile = spacePath + "/" + dir + "/" + col + FileConfig.DESC_FILE_SUFFIX;
                    List<String> list = FileUtil.readAll(descFile, false);
                    if (!CommonUtil.isEmpty(list)) {
                        info = new IndexInfo(list);
                        TableCache.putIndex(dataBase, table, dir, col, info);
                    }
                }
            } finally {
                lock.unlock();
            }
        } else {
            String descFile = spacePath + "/" + dir + "/" + col + FileConfig.DESC_FILE_SUFFIX;
            ReentrantLock lock = FileLockUtil.getLock(descFile);
            lock.lock();
            try {
                info = TableCache.getIndex(dataBase, table, dir, col);
                if (info != null) {
                    return info;
                }
                //List<String> list = FileUtil.readAll(descFile, false);
                byte[] bytes = GZipUtil.read2Byte(descFile);
                if (bytes == null) {
                    return null;
                }
                //未合并的索引文件，压缩前是 未压缩， 压缩后 是压缩的, 所以兼容掉
                byte[] uncompress = SnappyUtil.decompressWithCheck(bytes);
                if (uncompress != null) {
                    bytes = uncompress;
                }
                List<String> list = getLinesbyBytes(bytes);

                //queryTimeLogger.debug("read index in file, time:" + (System.currentTimeMillis() - s));
                if (CommonUtil.isEmpty(list)) {
                    return null;
                }
                info = new IndexInfo(list);
                if (Params.isSlave()) { //如果是从库只有满的DP索引文件才放入缓存
                    if (info.getCount() == ServerConstants.PARK_SIZ) {
                        TableCache.putIndex(dataBase, table, dir, col, info);
                    }
                } else {    //如果是主库直接将索引放入缓存
                    TableCache.putIndex(dataBase, table, dir, col, info);
                }
            } finally {
                lock.unlock();
            }
        }

        return info;
    }

    /**
     * 对于 数字类型的列，主动加载索引缓存
     * @param desc
     * @throws Exception
     */
    public static boolean createIndexInfo(String desc, byte[] bytes) throws Exception {
        String subPath = desc.substring(Params.getBaseDir().length());
        String[] strs = subPath.split("/");
        String dataBase = strs[0];
        String table = strs[1];
        String dir = strs[4];
        String col = strs[5].replace(FileConfig.DESC_FILE_SUFFIX, "");
        TableInfo tableInfo = CacheUtil.readTableInfo(dataBase, table);
        if (tableInfo == null) {
            return false;
        }
        //如果不是数值或者枚举类型的，不缓存
        /*if (!tableInfo.isIndexCol(col)) {
            return false;
        }*/
        if (!Params.getLoadFields().contains(col)) {
            return false;
        }
        IndexInfo info = TableCache.getIndex(dataBase, table, dir, col);
        if (info == null) {
            List<String> list = null;
            if (bytes == null) {
                list = FileUtil.readAll(desc, false);
            } else {
                list = new ArrayList<String>();
                int lineStart = 0;
                for (int i = 0, k = bytes.length; i < k; i++) {
                    if (bytes[i] == ServerConstants.LINE_SEPARATOR) {
                        list.add(new String(bytes, lineStart, i - lineStart));
                        lineStart = i + 1;
                    }
                }
            }
            if (CommonUtil.isEmpty(list)) {
                return false;
            }
            info = new IndexInfo(list);
            if (info.getCount() == ServerConstants.PARK_SIZ) {    //如果是满块的 加载到缓存中
                TableCache.putIndex(dataBase, table, dir, col, info);
                return true;
            }
        }
        return false;
    }

    private static List<String> getLinesbyBytes(byte[] bytes) {
        List<String> list = new ArrayList<String>();
        int lineStart = 0;
        for (int i = 0, k = bytes.length; i < k; i++) {
            if (bytes[i] == ServerConstants.LINE_SEPARATOR) {
                list.add(new String(bytes, lineStart, i - lineStart));
                lineStart = i + 1;
            }
        }
        return list;
    }

    /**
     * 读取表列定义
     * @param table
     * @return
     * @throws Exception
     */
    public static TableInfo readTableInfo(String dataBase, String table) throws Exception {
        String tablePath = DataBaseUtil.getTablePath(dataBase, table);
        TableInfo cache = TableCache.getTableInfo(dataBase, table);
        if (cache != null) {
            return cache;
        }
        String tableDirDesc = tablePath + "/desc.txt";
        List<String> columnList = FileUtil.readAll(tableDirDesc, false);
        if (CommonUtil.isEmpty(columnList)) {
            return null;
        }
        ColumnDef[] cols = new ColumnDef[columnList.size()];
        for (int i = 0, k = columnList.size(); i < k; i++) {
            cols[i] = new ColumnDef(columnList.get(i));
        }
        String seqTxt = tablePath + "/" + Constants.TABLE_SEQUENCE;
        List<String> list = FileUtil.readAll(seqTxt, false);
        long seq = 0;
        if (!CommonUtil.isEmpty(list)) {
            seq = CommonUtil.parseLong(list.get(0));
        }
        TableInfo tableInfo = new TableInfo(cols, seq);
        TableCache.putTableInfo(dataBase, table, tableInfo);
        return tableInfo;
    }

    public static EnumInfo getEnumInfo(String dataBase, String table, String col) throws Exception {
        EnumInfo enumInfo = TableCache.getEnumInfo(dataBase, table, col);
        if (enumInfo != null) {
            return enumInfo;
        }
        String tablePath = DataBaseUtil.getTablePath(dataBase, table);
        String colPath = tablePath + "/" + col + Constants.COL_ENUM_TXT;
        List<String> lines = FileUtil.readAll(colPath, false);
        if (CommonUtil.isEmpty(lines)) {
            return null;
        }
        enumInfo = new EnumInfo(lines);
        TableCache.putEnumInfo(dataBase, table, col, enumInfo);
        return enumInfo;
    }

    public static void main(String args[]) throws Exception {
        String s = getEnumInfo("test_database", "test_table", "menu").getData(78L);
        System.out.println(s);
    }

}
