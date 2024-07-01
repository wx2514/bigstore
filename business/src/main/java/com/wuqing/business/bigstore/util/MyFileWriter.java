package com.wuqing.business.bigstore.util;


import java.io.*;
import java.nio.charset.Charset;

public class MyFileWriter extends OutputStreamWriter {

    /**
     * Constructs a FileWriter object given a file name.
     *
     * @param fileName  String The system-dependent filename.
     * @throws IOException  if the named file exists but is a directory rather
     *                  than a regular file, does not exist but cannot be
     *                  created, or cannot be opened for any other reason
     */
    public MyFileWriter(String fileName) throws IOException {
        super(new FileOutputStream(fileName));
    }

    /**
     * Constructs a FileWriter object given a file name with a boolean
     * indicating whether or not to append the data written.
     *
     * @param fileName  String The system-dependent filename.
     * @param append    boolean if <code>true</code>, then data will be written
     *                  to the end of the file rather than the beginning.
     * @throws IOException  if the named file exists but is a directory rather
     *                  than a regular file, does not exist but cannot be
     *                  created, or cannot be opened for any other reason
     */
    public MyFileWriter(String fileName, boolean append) throws IOException {
        super(new FileOutputStream(fileName, append));
    }

    /**
     * 追加字符集
     * @param fileName
     * @param append
     * @param charsetName
     * @throws IOException
     */
    public MyFileWriter(String fileName, boolean append, Charset charsetName) throws IOException {
        super(new FileOutputStream(fileName, append), charsetName);
    }

    /**
     * Constructs a FileWriter object given a File object.
     *
     * @param file  a File object to write to.
     * @throws IOException  if the file exists but is a directory rather than
     *                  a regular file, does not exist but cannot be created,
     *                  or cannot be opened for any other reason
     */
    public MyFileWriter(File file) throws IOException {
        super(new FileOutputStream(file));
    }

    /**
     * Constructs a FileWriter object given a File object. If the second
     * argument is <code>true</code>, then bytes will be written to the end
     * of the file rather than the beginning.
     *
     * @param file  a File object to write to
     * @param     append    if <code>true</code>, then bytes will be written
     *                      to the end of the file rather than the beginning
     * @throws IOException  if the file exists but is a directory rather than
     *                  a regular file, does not exist but cannot be created,
     *                  or cannot be opened for any other reason
     * @since 1.4
     */
    public MyFileWriter(File file, boolean append) throws IOException {
        super(new FileOutputStream(file, append));
    }

    /**
     * 追加字符集
     * @param file
     * @param append
     * @param charsetName
     * @throws IOException
     */
    public MyFileWriter(File file, boolean append, Charset charsetName) throws IOException {
        super(new FileOutputStream(file, append), charsetName);
    }

    /**
     * Constructs a FileWriter object associated with a file descriptor.
     *
     * @param fd  FileDescriptor object to write to.
     */
    public MyFileWriter(FileDescriptor fd) {
        super(new FileOutputStream(fd));
    }

}
