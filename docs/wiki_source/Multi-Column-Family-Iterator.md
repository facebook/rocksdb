## Introduction

MultiCfIterator enables traversal across keys from various column families. As of version 9.2.0, it maintains all functionalities of the Iterator except for `Refresh()` It provides [consistent-view](https://github.com/facebook/rocksdb/wiki/Iterator#consistent-view) across all column families in the same way that Iterator does (explicit, if ReadOptions.snapshot is set. Otherwise implicit snapshot as of the time the iterator is created). MultiCfIterator has the same limitation in [prefix iteration](https://github.com/facebook/rocksdb/wiki/Iterator#prefix-iterating).

MultiCfIterator is available in two variants: **CoalescingIterator** and **AttributeGroupIterator**.

## CoalescingIterator

CoalescingIterator implements the standard Iterator interface, including `value()` and `columns()`. Below is an example of how to instantiate a `CoalescingIterator` for three column families:

```cpp
ReadOptions ro;
ro.iterate_lower_bound = lower_bound; // optional lower bound
ro.iterate_upper_bound = upper_bound; // optional upper bound

std::vector<ColumnFamilyHandle*> cfhs{cf_1_handle, cf_2_handle, cf_3_handle};

std::unique_ptr<Iterator> iter = db_->NewCoalescingIterator(ro, cfhs);

for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
  // Do something with iter->key() and iter->value()
}
```

### Handling the same key in multiple column families

If the same key is present in multiple column families, the value from the last specified column family in the iterator creation will take precedence. 

For example, if the key "foo" appears in both `cf_1` and `cf_3` with values "bar" and "baz" respectively, `value()` will return "baz" when the iterator is positioned at "foo".

For Wide Columns accessed via `columns()`, they are merged into a single list. If a wide column with the same name exists in multiple column families, the last one specified takes precedence. 

For instance, if the key "k" appears in `cf_1`, `cf_2`, and `cf_3` with respective wide columns 

- cf_1: `{"col_1": "cf_1_val_1", "col_2": "cf_1_val_2"}`
- cf_2: `{"col_1": "cf_2_val_1", "col_3": "cf_2_val_3"}`
- cf_3: `{"col_1": "cf_3_val_1", "col_4": "cf_3_val_4"}`, 

then when the iterator is at "k", `columns()` will return the following: 

`{"col_1": "cf_3_val_1", "col_2": "cf_1_val_2", "col_3": "cf_2_val_3", "col_4": "cf_3_val_4"}`. 

Note that the information about which column family `value()` or `columns()` belong to is not retained in CoalescingIterator. If this information is needed, or if all values/columns for keys existing in more than one column family are needed, consider using the AttributeGroupIterator.


## AttributeGroupIterator

Unlike CoalescingIterator, AttributeGroupIterator does not provide `value()` or `columns()`. Instead, it offers `attribute_groups()`, where each `AttributeGroup` represents a collection of wide columns grouped by column family. This allows identification of which wide columns are associated with which column family. Below is an example of how to set up an `AttributeGroupIterator` for three column families:

```cpp
ReadOptions ro;
ro.iterate_lower_bound = lower_bound; // optional lower bound
ro.iterate_upper_bound = upper_bound; // optional upper bound

std::vector<ColumnFamilyHandle*> cfhs{cf_1_handle, cf_2_handle, cf_3_handle};

std::unique_ptr<AttributeGroupIterator> iter = db_->NewAttributeGroupIterator(ro, cfhs);
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
  for (auto attribute_group : iter->attribute_groups()) {
    // Do something with iter->key() and attribute_group->columns();
  }  
}
```
