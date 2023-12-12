* Java API extensions to improve consistency and completeness of APIs
  1 Extended `RocksDB.get([ColumnFamilyHandle columnFamilyHandle,] ReadOptions opt, ByteBuffer key, ByteBuffer value)` which now accepts indirect buffer parameters as well as direct buffer parameters
  2 Extended `RocksDB.put( [ColumnFamilyHandle columnFamilyHandle,] WriteOptions writeOpts, final ByteBuffer key, final ByteBuffer value)` which now accepts indirect buffer parameters as well as direct buffer parameters
  3 Added `RocksDB.merge([ColumnFamilyHandle columnFamilyHandle,] WriteOptions writeOptions, ByteBuffer key, ByteBuffer value)` methods with the same parameter options as `put(...)` - direct and indirect buffers are supported
  4 Added `RocksIterator.key( byte[] key [, int offset, int len])` methods which retrieve the iterator key into the supplied buffer
  5 Added `RocksIterator.value( byte[] value [, int offset, int len])` methods which retrieve the iterator value into the supplied buffer
  6 Deprecated `get(final ColumnFamilyHandle columnFamilyHandle, final ReadOptions readOptions, byte[])` in favour of `get(final ReadOptions readOptions, final ColumnFamilyHandle columnFamilyHandle, byte[])` which has consistent parameter ordering with other methods in the same class
  7 Added `Transaction.get( ReadOptions opt, [ColumnFamilyHandle columnFamilyHandle, ] byte[] key, byte[] value)` methods which retrieve the requested value into the supplied buffer
  8 Added `Transaction.get( ReadOptions opt, [ColumnFamilyHandle columnFamilyHandle, ] ByteBuffer key, ByteBuffer value)` methods which retrieve the requested value into the supplied buffer
  9 Added `Transaction.getForUpdate( ReadOptions readOptions, [ColumnFamilyHandle columnFamilyHandle, ] byte[] key, byte[] value, boolean exclusive [, boolean doValidate])` methods which retrieve the requested value into the supplied buffer
  10 Added `Transaction.getForUpdate( ReadOptions readOptions, [ColumnFamilyHandle columnFamilyHandle, ] ByteBuffer key, ByteBuffer value, boolean exclusive [, boolean doValidate])` methods which retrieve the requested value into the supplied buffer
  11 Added `Transaction.getIterator()` method as a convenience which defaults the `ReadOptions` value supplied to existing `Transaction.iterator()` methods. This mirrors the existing `RocksDB.iterator()` method.
  12 Added `Transaction.put([ColumnFamilyHandle columnFamilyHandle, ]  ByteBuffer key, ByteBuffer value [, boolean assumeTracked])` methods which supply the key, and the value to be written in a `ByteBuffer` parameter
  13 Added `Transaction.merge([ColumnFamilyHandle columnFamilyHandle, ] ByteBuffer key, ByteBuffer value [,  boolean assumeTracked])` methods which supply the key, and the value to be written/merged in a `ByteBuffer` parameter
  14 Added `Transaction.mergeUntracked([ColumnFamilyHandle columnFamilyHandle, ] ByteBuffer key, ByteBuffer value)` methods which supply the key, and the value to be written/merged in a `ByteBuffer` parameter
 
