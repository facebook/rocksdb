Java API `multiGet()` variants now take advantage of the underlying batched `multiGet()` performance improvements.

Before
```
Benchmark (columnFamilyTestType) (keyCount) (keySize) (multiGetSize) (valueSize) Mode Cnt Score Error Units
MultiGetBenchmarks.multiGetList10 no_column_family 10000 16 100 64 thrpt 25 6315.541 ± 8.106 ops/s
MultiGetBenchmarks.multiGetList10 no_column_family 10000 16 100 1024 thrpt 25 6975.468 ± 68.964 ops/s
```
After
```
Benchmark (columnFamilyTestType) (keyCount) (keySize) (multiGetSize) (valueSize) Mode Cnt Score Error Units
MultiGetBenchmarks.multiGetList10 no_column_family 10000 16 100 64 thrpt 25 7046.739 ± 13.299 ops/s
MultiGetBenchmarks.multiGetList10 no_column_family 10000 16 100 1024 thrpt 25 7654.521 ± 60.121 ops/s
```