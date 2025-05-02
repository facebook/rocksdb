* Fix an infinite-loop bug in transaction locking. This can happen if a transaction reaches lock limit and its time out expires before it attempts to wait for it.
