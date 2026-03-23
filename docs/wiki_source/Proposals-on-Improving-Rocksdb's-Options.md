We already have 84 options and they keep increasing. Along with the rise of number is the growing complexity: it's a impossible mission for a normal user to figure out what each option means -- not to mention the magical effect when combining some options together. Moreover, most of our users (eventually) will be made up by non-experts who interest more in getting things done quickly rather than understand RocksDB fully.

As we discussed, ideally we hope there is a "tuning adviser" that analyze the system dynamically and give improvement suggestions with little effort from user's side. That's a great goal, but to start with, I'll propose several simple ways that may enhance our users' life quality in a shorter time.

Below are the proposals. Each of them is relatively independent and can be the complement to the others.

### 1. Don't put all eggs in the same basket.

**Motivation:** now almost every tunable things are put into `struct Options`, leading to cohorts of unrelated and/or contradicting options.

For example, some options are related to block-based table (`block_size`, `block_cache`, `use_block_cache`, etc); some options only belong to a specific memtable_rep (`memtable_prefix_bloom_bits`); some make sense only to leveldb style compaction but useless for universal compaction, etc.

**Solution:** move out these incoherent options to a "better place".

For example: for block-based table, we have a dedicated "table options" which encapsulate the `block_size`, `block_size_deviation`. It is used like:
```c++
BlockBasedTableOptions table_options;
table_options.block_size = 12345; 
options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

Similarly, for other pluggable components like memtables, compaction algorithms, etc., we could also introduce specific options or just configure their behaviors in their constructors. For example, plain table can defines its behavior in the table factory constructor's parameters instead using the sub-options:

```c++
explicit PlainTableFactory(uint32_t user_key_len = kPlainTableVariableLength,
                           int bloom_bits_per_key = 0,
                           double hash_table_ratio = 0.75)
```

**API Change:** We will need to create new options and make radical api changes on `Options`; however at least when adding new "configurable things", we can put them in more specific places.

### 2. Separate basic options from advanced ones

**Motivation**: All options are equal, but some options are more equal than others. The overwhelming options plague both developers and ordinary users. To make study curves, we'd hide/remove the advanced options from most frequently used options.

**Solution**:

One tentative solution will be providing two sets of options:
```c++
struct Options;  // provide the most fundamental and commonly used options.
struct CompleteOptions;  // equivalent to current options, which has everything.

// DB::Open accepts both Options or CompleteOptions
leveldb::DB::Open(simple_options, "/tmp/testdb", &db);
leveldb::DB::Open(complete_options, "/tmp/testdb", &db);
```

There are several ways to achieve this. But regardless of the specific implementation, the message here is to be able to expose users with simplified interface while retaining the ability for advanced user to make finer tuning.

**API Change:** if we prefer `Options` to represent simplified options, then users will be affected significantly. Or we can let `Options` still be the complete options and give simplified options another name.

### 3. Provide a higher level of abstraction

**Motivation:** options don't always work alone. And the chemistry between them can be very complicated and uninteresting for users.

Say, you want to create a plain table, what would you do? you need to set 3 options to make it work, and in table_factory.

```c++
options_.allow_mmap_reads = true;
SliceTransform* prefix_extractor = new NewFixedPrefixTransform(8);
options_.prefix_extractor = prefix_extractor;
options.table_factory.reset(NewPlainTableFactory(8, 10));
```

And then you want universal compaction? OK, more options are needed.

OK, then I need the memtable that supports inplace update and hash linked list memtable rep. Even more options you will need to know.

So to achieve you'll be seeing a full page of `options.blah_blah = blah_blah_blah;`.

**Solution**:

Instead of using options directly, maybe we can offer a higher level of abstraction that facilitate the whole process.

What do user want? In a plain language, they want:

1. A plain table to that use first 8 bytes as the prefix; plain table's bloom filter should be also enabled for faster access;
2. Use universal compaction to reduce the write amplification.
3. Use hash linked list memtable rep and enable inplace update.

What if we do something like this (please ignore the grammar, let's focus on the semantic):
```c++
OptionsBuilder options_builder;
options_builder.UsePlainTable(8 /* prefix size */, 10 /* bloom filter */)
               .UseUniversalCompaction()
               .UseHashLinkedListMemTable(8 /* use 8 bytes prefix for hashing */)
               .EnableInplaceMemtableUpdate();
auto options = options_builder.Build();
```
That looks nicer and of more fun than writing 20~30 lines of `options.blah = blah`. We don't necessarily need to pack all these things into one single `OptionsBuilder`; instead we can have sub-types of OptionsBuilder. But again, implementation details don't matter at this stage :-)

**And we can also do the verification when we are building optoins**. Not all these intents are compatible with each other. For example, you cannot specify plain table and block-based table; universal compaction may work poorly with some type of table format (I'm just making up an example).

### More proposals
By aggregating the feedback from other team members, there are also some more complicated yet powerful directions to go:

1. Getting the options used by all Facebook's rocksdb instances and their environment (qps, memory, cpu). These can help us better pinpoint how people use rocksdb and how can we improve them.
2. Automatic options advisor, which can ask user questions and then generate the options accordingly. Also it will do dynamic analysis on the system and give options improvement suggestions for users.
