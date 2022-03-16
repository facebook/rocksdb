ifndef PYTHON

# Default to python3. Some distros like CentOS 8 do not have `python`.
ifeq ($(origin PYTHON), undefined)
	PYTHON := $(shell which python3 || which python || echo python3)
endif
export PYTHON

endif
