package com.wuqing.business.bigstore.util;


import com.wuqing.business.bigstore.config.PressConstants;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.CommonUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import net.jpountz.lz4.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 通过Java的GZip，GZip2输入输出流实现压缩和解压文件
 *
 * @author liujiduo
 *
 */
public final class GZipUtil {

    private final static Logger logger = LoggerFactory.getLogger(GZipUtil.class);

    private final static Logger queryDetailLogger = LoggerFactory.getLogger("query-detail-log");

    private static final int BUFFER = 1024;

    public static void compress(String filePath) {
        compress(filePath, false);
    }

    public static void compress(String filePath, boolean uncompress) {
        File file = new File(filePath);
        if (!file.exists()) {   //如果不存在直接返回
            return;
        }
        int idx = filePath.lastIndexOf(".");
        String suffix = null;
        if (PressConstants.COMPRESS_TYPE == PressConstants.GZIP_TYPE) {
            suffix = PressConstants.GZ_TYPE;
        } else if (PressConstants.COMPRESS_TYPE == PressConstants.XZ_TYPE) {
            suffix = PressConstants.XZ_FILE_TYPE;
        } else if (PressConstants.COMPRESS_TYPE == PressConstants.BZIP2_TYPE) {
            suffix = PressConstants.BZ2_FILE_TYPE;
        } else {
            return; //不压缩.
        }
        String outFilePath = null;
        if (idx > -1) {
            outFilePath = filePath.substring(0, idx) + suffix;
        } else {
            outFilePath = filePath + suffix;
        }
        //InputStream is = null;
        OutputStream os = null;
        try {
            byte[] bts = readTxt2Byte(file);
            if (uncompress) {
                byte[] uncompres = SnappyUtil.decompressWithCheck(bts);
                if (uncompres != null) {
                    bts = uncompres;
                }
            }
            //is = new FileInputStream(file);
            os = new FileOutputStream(outFilePath);
            if (PressConstants.COMPRESS_TYPE == PressConstants.GZIP_TYPE) {
                compressByGzip(bts, os);
            } else if (PressConstants.COMPRESS_TYPE == PressConstants.XZ_TYPE) {
                compressByXz(bts, os);
            } else if (PressConstants.COMPRESS_TYPE == PressConstants.BZIP2_TYPE) {
                compressByBzip2(bts, os);
            } else {
                return;
            }
        } catch (IOException e) {
            logger.error("compress fail, filePath:" + filePath, e);
        } finally {
            //CommonUtil.close(is);
            CommonUtil.close(os);
        }
    }

    private static void compressByXz(byte[] bts, OutputStream os) {
        XZCompressorOutputStream gos = null;
        try {
            gos = new XZCompressorOutputStream(os);
            gos.write(bts);
            gos.flush();
            gos.finish();
        } catch (IOException e) {
            logger.error("compress fail.", e);
        } finally {
            CommonUtil.close(gos);
        }
    }

    private static void compressByGzip(byte[] bts, OutputStream os) {
        GZIPOutputStream gos = null;
        try {
            gos = new GZIPOutputStream(os);
            gos.write(bts);
            gos.finish();
            gos.flush();
        } catch (IOException e) {
            logger.error("compress fail.", e);
        } finally {
            CommonUtil.close(gos);
        }
    }

    private static void compressByBzip2(byte[] bts, OutputStream os) {
        BZip2CompressorOutputStream gos = null;
        try {
            gos = new BZip2CompressorOutputStream(os);
            gos.write(bts);
            gos.flush();
            gos.finish();
        } catch (IOException e) {
            logger.error("compress fail.", e);
        } finally {
            CommonUtil.close(gos);
        }
    }

    /**
     * 数据解压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void decompress(InputStream is, OutputStream os) {
        GZIPInputStream gis = null;
        try {
            gis = new GZIPInputStream(is);
            int count;
            byte data[] = new byte[BUFFER];
            while ((count = gis.read(data, 0, BUFFER)) != -1) {
                os.write(data, 0, count);
            }
        } catch (IOException e) {
            logger.error("decompress fail", e);
        } finally {
            CommonUtil.close(gis);
        }
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static List<String> decompress2String(String filePath) {
        List<String> lines = new ArrayList<String>();
        FileInputStream is = null;
        GZIPInputStream gis = null;
        try {
            is = new FileInputStream(filePath);
            gis = new GZIPInputStream(is);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gis, Constants.DEFAULT_CHARSET));
            String lineTxt = null;
            while ((lineTxt = reader.readLine()) != null) {
                lines.add(lineTxt);
            }
        } catch (Exception e) {
            logger.error("decompress to string fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(gis);
            CommonUtil.close(is);
        }
        return lines;
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static byte[] decompressGzip2Byte(String filePath) {
        FileInputStream is = null;
        GZIPInputStream gis = null;
        byte[] buffer = null;
        int off = 0;
        try {
            is = new FileInputStream(filePath);
            gis = new GZIPInputStream(is);
            buffer = new byte[BUFFER * BUFFER];  //单次1M，后续根据需要扩容
            int count;
            boolean over = false;
            while ((count = gis.read(buffer, off, BUFFER)) != -1) {
                off += count;
                if (off + BUFFER > buffer.length) { //扩容
                    byte[] newBuffer = new byte[buffer.length * 2];     //2倍扩容
                    System.arraycopy(buffer, 0, newBuffer, 0, off);
                    buffer = newBuffer;
                }
            }
            return Arrays.copyOf(buffer, off);
        } catch (Exception e) {
            logger.error("decompress to byte fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(gis);
            CommonUtil.close(is);
        }
        return null;
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static byte[] decompressXz2Byte(String filePath) {
        FileInputStream is = null;
        XZCompressorInputStream gis = null;
        byte[] buffer = null;
        int off = 0;
        try {
            is = new FileInputStream(filePath);
            gis = new XZCompressorInputStream(is);
            buffer = new byte[BUFFER * BUFFER];  //单次1M，后续根据需要扩容
            int count;
            boolean over = false;
            while ((count = gis.read(buffer, off, BUFFER)) != -1) {
                off += count;
                if (off + BUFFER > buffer.length) { //扩容
                    byte[] newBuffer = new byte[buffer.length * 2];     //2倍扩容
                    System.arraycopy(buffer, 0, newBuffer, 0, off);
                    buffer = newBuffer;
                }
            }
            return Arrays.copyOf(buffer, off);
        } catch (Exception e) {
            logger.error("decompress to byte fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(gis);
            CommonUtil.close(is);
        }
        return null;
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static byte[] decompressBzip2Byte(String filePath) {
        FileInputStream is = null;
        BZip2CompressorInputStream gis = null;
        byte[] buffer = null;
        int off = 0;
        try {
            is = new FileInputStream(filePath);
            gis = new BZip2CompressorInputStream(is);
            buffer = new byte[BUFFER * BUFFER];  //单次1M，后续根据需要扩容
            int count;
            boolean over = false;
            while ((count = gis.read(buffer, off, BUFFER)) != -1) {
                off += count;
                if (off + BUFFER > buffer.length) { //扩容
                    byte[] newBuffer = new byte[buffer.length * 2];     //2倍扩容
                    System.arraycopy(buffer, 0, newBuffer, 0, off);
                    buffer = newBuffer;
                }
            }
            return Arrays.copyOf(buffer, off);
        } catch (Exception e) {
            logger.error("decompress to byte fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(gis);
            CommonUtil.close(is);
        }
        return null;
    }

    /**
     * 读取字节
     * @param filePath 文件路径
     * @return
     */
    public static byte[] read2Byte(String filePath) {
        long s = System.currentTimeMillis();
        byte[] bytes = null;
        if (filePath.endsWith(PressConstants.TXT_TYPE)) {    //txt文件不解压缩
            bytes =  readTxt2Byte(filePath);
        } else if (filePath.endsWith(PressConstants.GZ_TYPE)) {
            bytes = decompressGzip2Byte(filePath);
        } else if (filePath.endsWith(PressConstants.BZ2_FILE_TYPE)) {
            bytes = decompressBzip2Byte(filePath);
        } else if (filePath.endsWith(PressConstants.XZ_FILE_TYPE)) {
            bytes = decompressXz2Byte(filePath);
        } /*else {
            return null;
        }*/
        long time = (System.currentTimeMillis() - s);
        queryDetailLogger.debug("read-file-byte: path:" + filePath + ", time:" + time);
        return bytes;
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static byte[] readTxt2Byte(String filePath) {
        return readTxt2Byte(new File(filePath));
    }

    /**
     * 数据解压缩
     *
     * @param filePath
     * @throws Exception
     */
    public static byte[] readTxt2Byte(File filePath) {
        FileInputStream is = null;
        BufferedInputStream bis = null;
        byte[] buffer = null;
        int off = 0;
        try {
            is = new FileInputStream(filePath);
            bis = new BufferedInputStream(is);
            int length = (int) filePath.length();
            if (length == 0) {
                return new byte[0];
            }
            buffer = new byte[length];  //单词申请跟文件大小一样的数组，避免数组拷贝
            int count;
            while ((count = bis.read(buffer, off, length)) != -1) {
            }
            return buffer;
        } catch (Exception e) {
            logger.error("read txt to byte fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(bis);
            CommonUtil.close(is);
        }
        return null;
    }

    // 压缩
    public static byte[] compressByte(byte[] bytes) throws IOException {
        if (bytes == null) {
            return null;
        }
        ByteArrayOutputStream out = null;
        GZIPOutputStream gzip = null;
        try {
            out = new ByteArrayOutputStream();
            gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();
            return out.toByteArray();
        } catch (Exception e) {
            logger.error("compress String fail", e);
        } finally {
            CommonUtil.close(gzip);
            CommonUtil.close(out);
        }
        return null;
    }

    // 解压缩
    public static byte[] uncompressByte(byte[] bytes) throws IOException {
        if (bytes == null) {
            return null;
        }
        ByteArrayOutputStream out = null;
        ByteArrayInputStream in = null;
        GZIPInputStream gunzip = null;
        try {
            out = new ByteArrayOutputStream();
            in = new ByteArrayInputStream(bytes);
            gunzip = new GZIPInputStream(in);
            byte[] buffer = new byte[512];
            int n;
            while ((n = gunzip.read(buffer)) > 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
      } catch (Exception e) {
            logger.error("uncompress String fail", e);
        } finally {
            CommonUtil.close(out);
            CommonUtil.close(gunzip);
            CommonUtil.close(in);
        }
        return null;
    }

    public static byte[] lz4Compress(byte[] data) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, 8192, compressor);
        compressedOutput.write(data);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

    public static byte[] lz4Decompress(byte[] data) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
        LZ4FastDecompressor decompresser = factory.fastDecompressor();
        LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(data), decompresser);
        int count;
        byte[] buffer = new byte[8192];
        while ((count = lzis.read(buffer)) != -1) {
            baos.write(buffer, 0, count);
        }
        lzis.close();
        return baos.toByteArray();
    }

    public static void main(String[] args) throws Exception {
        /*read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000000/0000000089/time_data.bz");
        read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000000/0000000077/time_data.gz");
        read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000001/0000000109/time_data.xz");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            //read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000000/0000000089/time_data.bz");
            //read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000000/0000000077/time_data.gz");
            read2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000001/0000000109/time_data.xz");
        }*/
        //compress(new FileInputStream("/tmp/bigstore/test_database/test_table/rows/0/desc_data.txt"), new FileOutputStream("/tmp/bigstore/test_database/test_table/rows/0/desc_data.gz"));
        //decompress(new FileInputStream("/tmp/bigstore/test_database/test_table/rows/0/desc_desc.gz"), new FileOutputStream("/tmp/bigstore/test_database/test_table/rows/0/desc_desc.txt"));
        //decompress2String("/tmp/bigstore/test_database/test_table/rows/0/time_data.gz");
        //byte[] bytes = decompress2Byte("/tmp/bigstore/test_database/test_table/rows/0/time_data.gz");
        //System.out.println("time:" + (System.currentTimeMillis() - start));
        //compress("/tmp/bigstore/test_database/test_table/rows/0000000060/time_data.txt");
        /*for (int i = 0, k = bytes.length; i < k; i++) {
            byte b = bytes[i];
            if (b == '\n') {
                int n = 0;
                byte[] index = new byte[20];
                for (; n < 20 && i + n + 1 < k; n++) {
                    byte thisB = bytes[i + n + 1];
                    if (thisB == FileUtil.STACK_SPLIT) {
                        break;
                    }
                    index[n] = thisB;
                }
                index = Arrays.copyOf(index, n);
                //System.out.println(new String(index));
            }
        }*/
        //System.out.println("time:" + (System.currentTimeMillis() - start));
        //byte[] bytes = readTxt2Byte("/tmp/bigstore/default_data_base/test_table/rows/0000000003/name_data.txt");
        String str = "0,4\\,1T,1T,1T,38,1T,38,6@,38,1T,1T,1T,1T,38,38,38,1T,38,1T,1T,1T,6@,4\\,6@,1T,1T,1T,4\\,1T,6@,38,38,38,4\\,6@,1T,1T,38,4\\,6@,1T,1T,1T,1T,6@,1T,1T,1T,1T,38,38,4\\,1T,1T,1T,1T,1T,1T,9H,38,1T,1T,38,1T,1T,1T,4\\,38,1T,1T,1T,38,1T,1T,1T,1T,4\\,1T,1T,4\\,38,1T,1T,1T,38,6@,1T,4\\,1T,1T,1T,1T,1T,7d,1T,1T,38,1T,4\\,4\\,38,38,4\\,1T,38,4\\,4\\,38,7d,4\\,7d,1T,1T,1T,9H,1T,38,1T,4\\,1T,4\\,38,1T,1T,6@,38,38,1T,38,38,6@,1T,1T,4\\,4\\,1T,4\\,1T,38,38,6@,1T,1T,4\\,7d,1T,:l,1T,1T,1T,1T,1T,1T,1T,38,1T,1T,1T,38,4\\,38,1T,4\\,1T,1T,1T,38,1T,1T,4\\,38,1T,1T,6@,38,38,1T,1T,9H,1T,6@,1T,4\\,1T,1T,1T,1T,38,4\\,1T,1T,1T,38,1T,1T,1T,1T,1T,1T,38,7d,38,1T,38,1T,38,38,1T,1T,1T,1T,1T,1T,1T,1T,4\\,1T,1T,1T,38,1T,38,1T,:l,4\\,1T,1T,38,38,1T,1T,7d,38,6@,38,38,1T,9H,38,4\\,6@,4\\,4\\,1T,38,1T,6@,38,1T,1T,1T,1T,1T,38,6@,1T,4\\,38,38,1T,1T,1T,1T,1T,1T,1T,38,1T,4\\,1T,1T,6@,<P,38,1T,38,38,7d,1T,4\\,4\\,1T,38,1T,38,38,1T,1T,38,1T,1T,1T,38,1T,4\\,38,1T,1T,38,1T,38,6@,1T,4\\,6@,7d,38,1T,4\\,1T,4\\,1T,1T,1T,1T,38,4\\,1T,1T,38,1T,9H,1T,38,1T,1T,38,38,38,1T,1T,1T,38,1T,1T,38,:l,38,9H,4\\,1T,1T,1T,1T,1T,38,1T,4\\,4\\,4\\,38,1T,1T,1T,1T,1T,38,6@,38,6@,1T,38,1T,1T,1T,1T,38,1T,4\\,1T,7d,38,1T,1T,38,1T,1T,9H,7d,38,38,1T,38,1T,38,1T,1T,1T,4\\,9H,1T,1T,38,1T,1T,4\\,38,1T,38,1T,4\\,6@,38,4\\,<P,9H,38,4\\,38,38,38,38,1T,4\\,38,1T,38,1T,6@,4\\,38,1T,1T,4\\,1T,1T,7d,1T,38,38,1T,9H,1T,38,4\\,1T,38,7d,38,1T,38,38,1T,1T,1T,1T,7d,1T,1T,1T,38,1T,7d,1T,1T,4\\,1T,7d,9H,1T,1T,1T,1T,1T,38,1T,1T,1T,4\\,1T,1T,4\\,1T,1T,1T,1T,6@,4\\,1T,1T,7d,38,38,38,1T,38,38,38,1T,38,1T,38,4\\,1T,38,4\\,1T,38,38,1T,38,1T,1T,1T,1T,1T,1T,1T,38,6@,1T,1T,38,1T,1T,1T,38,1T";
        //System.out.println(str.length());
        long s = System.currentTimeMillis();
        for (int i = 0, k = 100000; i < k; i++) {
            byte[] org = str.getBytes();
            byte[] compressStr = compressByte(org);
            //System.out.println(org.length + " > " + compressStr.length);
            //byte[] compressStr = SnappyUtil.compress(str.getBytes());
            //System.out.println(compressStr.length);
            byte[] uncompress = uncompressByte(compressStr);
            //String uncompressStr = new String(uncompress);
            //System.out.println(uncompressStr);
            //String uncompressStr = new String(SnappyUtil.decompress(compressStr));
            //System.out.println(uncompressStr.length());
        }
        System.out.println("time:" + (System.currentTimeMillis() - s));

    }
}
