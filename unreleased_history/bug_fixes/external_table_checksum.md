Pass wrapped WritableFileWriter pointer to ExternalTableBuilder so that the file checksum can be correctly calculated and returned by SstFileWriter for external table files.
