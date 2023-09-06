package org.rocksdb;

public class FileCheckSum {

    private int fileNumber;
    private String checksums;
    private String checksumFuncName;

    public FileCheckSum(int fileNumber, String checksums, String checksumFuncName) {
        this.fileNumber = fileNumber;
        this.checksums = checksums;
        this.checksumFuncName = checksumFuncName;
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    public String getChecksums() {
        return checksums;
    }

    public void setChecksums(String checksums) {
        this.checksums = checksums;
    }

    public String getChecksumFuncName() {
        return checksumFuncName;
    }

    public void setChecksumFuncName(String checksumFuncName) {
        this.checksumFuncName = checksumFuncName;
    }
}
