Fix a bug in auto_readahead_size where first_internal_key of index blocks wasn't copied properly resulting in corruption error when first_internal_key was used for comparison.
