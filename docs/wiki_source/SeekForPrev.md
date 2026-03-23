## SeekForPrev API
Start from 4.13, Rocksdb added `Iterator::SeekForPrev()`. This new API will seek to the last key that is less than or equal to the target key, in contrast with `Seek()`.
```cpp
// Suppose we have keys "a1", "a3", "b1", "b2", "c2", "c4".
auto iter = db->NewIterator(ReadOptions());
iter->Seek("a1");        // iter->Key() == "a1";
iter->Seek("a3");        // iter->Key() == "a3";
iter->SeekForPrev("c4"); // iter->Key() == "c4";
iter->SeekForPrev("c3"); // iter->Key() == "c2";
```
Basically, the behavior of SeekForPrev() is like the code snippet below:
```cpp
Seek(target); 
if (!Valid()) {
  SeekToLast();
} else if (key() > target) { 
  Prev(); 
}
```
In fact, this API serves more than this code snippet. One typical example is, suppose we have keys, "a1", "a3", "a5", "b1" and enable prefix_extractor which take the first byte as prefix. If we want to get the last key less than or equal to "a6". The code above without SeekForPrev() doesn't work. Since after `Seek("a6")` and `Prev()`, the iterator will enter the invalid state. But now, what you need is just a call, `SeekForPrev("a6")` and get "a5".

Also, SeekForPrev() API serves internally to support `Prev()` in prefix mode. Now, `Next()` and `Prev()` could both be mixed in prefix mode. Thanks to SeekForPrev()!