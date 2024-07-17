* Fix a major bug in which an iterator using prefix filtering and SeekForPrev might miss data when the DB is using `whole_key_filtering=false` and `partition_filters=true`.
