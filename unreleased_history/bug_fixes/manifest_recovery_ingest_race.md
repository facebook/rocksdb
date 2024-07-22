Fixed a race between error recovery due to manifest sync or write failure and external SST file ingestion. Both attempt to write a new manifest file, which causes an assertion failure.
