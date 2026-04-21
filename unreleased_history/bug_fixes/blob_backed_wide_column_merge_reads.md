Fixed blob-backed wide-column merge reads to preserve correct status
propagation and resolution across memtable, read-only, and secondary DB
paths. As part of this fix, secondary `GetMergeOperands()` now returns `OK`
when it successfully returns raw merge operands, matching the primary/read-only
DB behavior instead of returning `MergeInProgress`.
