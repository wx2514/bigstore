package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.PressConstants;
import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.config.FileConfig;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 16/7/18.
 */
public class FileUtil {

    private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    private final static Logger queryDetailLogger = LoggerFactory.getLogger("query-detail-log");

    public final static String STACK_SPLIT_STR = String.valueOf(Constants.STACK_SPLIT);

    /**
     * 按行读取所有文件
     * @param filePath
     * @return 返回集合，一行为一个字符串
     */
    public static List<String> readAll(String filePath, boolean includeBlank) throws Exception {
        List<String> lineList = new ArrayList<String>();
        InputStreamReader read = null;
        BufferedReader bufferedReader = null;
        try {
            File file = new File(filePath);
            if (file.isFile() && file.exists()) { //判断文件是否存在
                read = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET);
                bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int i = 0;
                //空行不算
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    if ("".equals(lineTxt)) {
                        if (includeBlank) {
                            lineList.add(lineTxt);
                        }
                        continue;
                    }
                    lineList.add(lineTxt);
                }
            } else {
                //logger.warn("找不到指定文件:" + filePath);
                return null;
            }
        } finally {
            IOUtils.closeQuietly(bufferedReader);
            IOUtils.closeQuietly(read);
        }
        return lineList;
    }

    /**
     * 读取文件第N行
     *
     * @param file
     * @param lineNumber 第几行
     * @return 指定行的文件
     */
    public static String readLine(File file, int lineNumber) throws Exception {
        if (file == null) {
            return null;
        }
        InputStreamReader read = null;
        BufferedReader bufferedReader = null;
        try {
            if (file.isFile() && file.exists()) { //判断文件是否存在
                read = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET);
                bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int i = 0;
                //空行不算
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    if (i++ == lineNumber) {
                        return lineTxt;
                    }
                }
            } else {
                logger.warn("找不到指定文件:" + file.getPath());
                return null;
            }
        } finally {
            IOUtils.closeQuietly(bufferedReader);
            IOUtils.closeQuietly(read);
        }
        return null;
    }

    /**
     * 读取文件第N行
     *
     * @param filePath
     * @param lineNumber 第几行
     * @return 指定行的文件
     */
    public static String readLine(String filePath, int lineNumber) throws Exception {
        if (filePath == null) {
            return null;
        }
        return readLine(new File(filePath), lineNumber);
    }

    /**
     * 复制单个文件
     *
     * @param srcFileName  待复制的文件名
     * @param destFileName 目标文件名
     * @param overlay      如果目标文件存在，是否覆盖
     * @return 如果复制成功返回true，否则返回false
     */
    public static void copyFile(String srcFileName, String destFileName,
                                   boolean overlay) {
        File srcFile = new File(srcFileName);
        // 判断源文件是否存在
        if (!srcFile.exists()) {
            throw new RuntimeException("源文件：" + srcFileName + "不存在！");
        } else if (!srcFile.isFile()) {
            throw new RuntimeException("复制文件失败，源文件：" + srcFileName + "不是一个文件！");
        }
        // 判断目标文件是否存在
        File destFile = new File(destFileName);
        if (destFile.exists()) {
            // 如果目标文件存在并允许覆盖
            if (overlay) {
                // 删除已经存在的目标文件，无论目标文件是目录还是单个文件
                new File(destFileName).delete();
            }
        } else {
            // 如果目标文件所在目录不存在，则创建目录
            if (!destFile.getParentFile().exists()) {
                // 目标文件所在目录不存在
                if (!destFile.getParentFile().mkdirs()) {
                    // 复制文件失败：创建目标文件所在目录失败
                    throw new RuntimeException("复制文件失败，创建目标文件所在目录失败");
                }
            }
        }
        // 复制文件
        int byteread = 0; // 读取的字节数
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(srcFile);
            out = new FileOutputStream(destFile);
            byte[] buffer = new byte[1024];

            while ((byteread = in.read(buffer)) != -1) {
                out.write(buffer, 0, byteread);
            }
        } catch (Exception e) {
            logger.error("copy file fail.", e);
            throw new RuntimeException("拷贝文件失败，出现异常.", e);
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
        }
    }

    public static void writeFile(String fileName, String text, boolean append) {
        FileOutputStream fop = null;
        try {
            File file = new File(fileName);
            //如果父目录不存在，则创建
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                throw new RuntimeException("创建父目录失败,fileName:" + fileName);
            }
            //如果文件不存在，则创建
            if (!file.exists() && !file.createNewFile()) {
                throw new RuntimeException("创建文件失败,fileName:" + fileName);
            }
            fop = new FileOutputStream(file, append);
            // get the content in bytes
            byte[] contentInBytes = text.getBytes();
            fop.write(contentInBytes);
            fop.flush();
        } catch (IOException e) {
            logger.error("write file fail.", e);
            throw new RuntimeException("写文件失败.", e);
        } finally {
            IOUtils.closeQuietly(fop);
        }
    }

    public static void writeFile(String fileName, List<String> textList, boolean append) {
        MyFileWriter fw = null;
        BufferedWriter writer = null;
        try {
            File file = new File(fileName);
            //如果父目录不存在，则创建
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                throw new RuntimeException("创建父目录失败,fileName:" + fileName);
            }
            //如果文件不存在，则创建
            if (!file.exists() && !file.createNewFile()) {
                throw new RuntimeException("创建文件失败,fileName:" + fileName);
            }
            fw = new MyFileWriter(file, append, Constants.DEFAULT_CHARSET);
            writer = new BufferedWriter(fw);
            // get the content in bytes
            for (String text : textList) {
                if (text == null) {
                    text = "";
                }
                writer.write(text);
                //writer.newLine();
                writer.write(Constants.LINE_SEPARATOR);
            }
            writer.flush();
        } catch (IOException e) {
            logger.error("write file fail.", e);
            throw new RuntimeException("写文件失败.", e);
        } finally {
            IOUtils.closeQuietly(fw);
            IOUtils.closeQuietly(writer);
        }
    }

    public static int writeData(String fileName, List textList, int start, boolean append, int length, boolean isNumber) {
        MyFileWriter fw = null;
        BufferedWriter writer = null;
        int nullCount = 0;
        try {
            File file = new File(fileName);
            //如果父目录不存在，则创建
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                throw new RuntimeException("创建父目录失败,fileName:" + fileName);
            }
            //如果文件不存在，则创建
            if (!file.exists() && !file.createNewFile()) {
                throw new RuntimeException("创建文件失败,fileName:" + fileName);
            }
            fw = new MyFileWriter(file, append, Constants.DEFAULT_CHARSET);
            writer = new BufferedWriter(fw);
            DecimalFormat lineDf =  DataBaseUtil.getLineNumFormat();
            // get the content in bytes
            for (int i = 0, k = textList.size(); i < k; i++) {
                Object text = textList.get(i);
                if (text == null || "".equals(text)) {
                    nullCount++;
                    continue;
                }
                String line = null;
                long lineNum = i + start;
                if (ServerConstants.USE_64) {
                    line = DataBaseUtil.formatData(lineNum, ServerConstants.NUM_LENGTH, true);
                } else {
                    line = lineDf.format(lineNum);
                }
                writer.write(line + STACK_SPLIT_STR);
                String data = DataBaseUtil.formatData(text, length, isNumber);
                writer.write(String.valueOf(data));
                writer.write(Constants.LINE_SEPARATOR);
            }
            writer.flush();
        } catch (IOException e) {
            logger.error("write file fail.", e);
            throw new RuntimeException("写文件失败.", e);
        } finally {
            IOUtils.closeQuietly(fw);
            IOUtils.closeQuietly(writer);
        }
        return nullCount;
    }

    /**
     * 写指定的行数据
     * @param fileName
     * @param text
     * @param line
     */
    public static void writeLine(String fileName, String text, int line) throws Exception {
        List<String> lines = readAll(fileName, true);
        int index = 0;
        for (int k = lines.size(); index < k; index++) {
            if (index == line) {
                lines.set(index, text);
            }
        }
        for (; index <= line; index++) {
            if (index < line) {
                lines.add("");
            } else if (index == line) {
                lines.add(text);
                break;
            }
        }
        writeFile(fileName, lines, false);
    }

    /**
     * 删除文件或目录
     * @param file
     */
    public static void deleteFile(File file) {
        if (file.exists()) {//判断文件是否存在
            if (file.isFile()) {//判断是否是文件
                file.delete();//删除文件
            } else if (file.isDirectory()) {//否则如果它是一个目录
                File[] files = file.listFiles();//声明目录下所有的文件 files[];
                for (int i = 0;i < files.length;i ++) {//遍历目录下所有的文件
                    deleteFile(files[i]);//把每个文件用这个方法进行迭代
                }
                file.delete();//删除文件夹
            }
        } else {
            logger.error("所删除的文件不存在");
        }
    }

    public static boolean dataFileExists(String file, int compress) {
        if (file.endsWith(FileConfig.DESC_FILE_SUFFIX)) {
            file = file.substring(0, file.length() - FileConfig.DESC_FILE_SUFFIX.length());
        }
        String subffix = null;
        String filePre = file + FileConfig.DATA_FILE_SUFFIX_PRE;
        String fileTxt = filePre + PressConstants.TXT_TYPE;
        if (new File(fileTxt).exists()) {
            return true;
        }
        if (compress == PressConstants.XZ_TYPE) {
            subffix = PressConstants.XZ_FILE_TYPE;
        } else if (compress == PressConstants.BZIP2_TYPE) {
            subffix = PressConstants.BZ2_FILE_TYPE;
        } else {
            subffix = PressConstants.GZ_TYPE;
        }
        String compressFile = filePre + subffix;
        return (new File(compressFile).exists());
    }

    public static void writeByte(String f, byte[] bytes) {
        writeByte(new File(f), bytes);
    }

    public static void writeByte(File f, byte[] bytes) {
        BufferedOutputStream bufferedStream = null;
        FileOutputStream fileStream = null;
        try {
            fileStream = new FileOutputStream(f);
            bufferedStream = new BufferedOutputStream(fileStream);
            bufferedStream.write(bytes);
            bufferedStream.flush();
        } catch (Exception e) {
            logger.error("write byte to file fail.", e);
        } finally {
            CommonUtil.close(fileStream);
            CommonUtil.close(bufferedStream);
        }
    }

    public static void writeByte(File f, boolean append, byte[]... bytes) {
        BufferedOutputStream bufferedStream = null;
        FileOutputStream fileStream = null;
        try {
            fileStream = new FileOutputStream(f, append);
            bufferedStream = new BufferedOutputStream(fileStream);
            for (byte[] bts : bytes) {
                bufferedStream.write(bts);
            }
            bufferedStream.flush();
        } catch (Exception e) {
            logger.error("write byte to file fail.", e);
        } finally {
            CommonUtil.close(fileStream);
            CommonUtil.close(bufferedStream);
        }
    }

    /**
     * 读取文件转 byte[]
     * @param filePath
     * @return
     */
    public static byte[] read2Byte(String filePath) {
        return read2Byte(new File(filePath));
    }

    /**
     * 读取文件转 byte[]
     * @param filePath
     * @return
     */
    public static byte[] read2Byte(File filePath) {
        long s = System.currentTimeMillis();
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
            queryDetailLogger.debug("read-file-byte: path:" + filePath.getPath() + ", time:" + (System.currentTimeMillis() - s));
            return buffer;
        } catch (Exception e) {
            logger.error("read txt to byte fail, filePath:" + filePath, e);
        } finally {
            CommonUtil.close(bis);
            CommonUtil.close(is);
        }
        return null;
    }


    public static void main(String[] args) {

    }


}
