When `DBOptions::avoid_unnecessary_blocking_io` is true, obsolete `OPTIONS-*` files found during DB open are deleted by background purge instead of synchronously on the opening thread.
