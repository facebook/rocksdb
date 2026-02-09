ifndef PYTHON

# Default to python3. Some distros like CentOS 8 do not have `python`.
ifeq ($(origin PYTHON), undefined)
	PYTHON := $(shell which python3 || which python || echo python3)
endif
export PYTHON

endif

# To setup tmp directory, first recognize some old variables for setting
# test tmp directory or base tmp directory. TEST_TMPDIR is usually read
# by RocksDB tools though Env/FileSystem::GetTestDirectory.
ifeq ($(TEST_TMPDIR),)
TEST_TMPDIR := $(TMPD)
endif
ifeq ($(TEST_TMPDIR),)
ifeq ($(BASE_TMPDIR),)
BASE_TMPDIR :=$(TMPDIR)
endif
ifeq ($(BASE_TMPDIR),)
BASE_TMPDIR :=/tmp
endif
# Use /dev/shm if it has the sticky bit set (otherwise, /tmp or other
# base dir), and create a randomly-named rocksdb.XXXX directory therein.
TEST_TMPDIR := $(shell f=/dev/shm; test -k $$f || f=$(BASE_TMPDIR); \
  perl -le 'use File::Temp "tempdir";'	                            \
    -e 'print tempdir("'$$f'/rocksdb.XXXX", CLEANUP => 0)')
endif
export TEST_TMPDIR
