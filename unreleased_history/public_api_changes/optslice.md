* `GetAllKeyVersions()` now interprets empty slices literally, as valid keys, and uses new `OptSlice` type default value for extreme upper and lower range limits.
* `DeleteFilesInRanges()` now takes `RangeOpt` which is based on `OptSlice`. The overload taking `RangePtr` is deprecated.
