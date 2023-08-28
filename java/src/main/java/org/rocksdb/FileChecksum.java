package org.rocksdb;

public class FileChecksum {
  private final int fileNumber;
  private final byte[] checksum;
  private final String checksumFuncName;

  public FileChecksum(int fileNumber, byte[] checksum, String checksumFuncName) {
    this.fileNumber = fileNumber;
    this.checksum = checksum;
    this.checksumFuncName = checksumFuncName;
  }

  public int getFileNumber() {
    return fileNumber;
  }

  public byte[] getChecksum() {
    return checksum;
  }

  public String getChecksumFuncName() {
    return checksumFuncName;
  }
}
