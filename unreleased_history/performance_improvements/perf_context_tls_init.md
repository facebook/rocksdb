Perf context counters and timers no longer call the thread-local init function of `perf_context` on every update. With `kEnableCount`, a counter update costs about 60% less.
