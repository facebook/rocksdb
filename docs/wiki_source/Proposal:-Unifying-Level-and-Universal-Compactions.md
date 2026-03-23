## Motivation
Currently, we observed the following needs in universal compactions:
* Avoid doubling in space during compaction.
* Allow universal compaction to be multi-threaded.

To satisfy the above needs, a reasonable solution is to partition large SST files in universal compaction into multiple smaller SST files, which allows deleting some of the SST files during the compaction to avoid doubling in space and provides multi-thread opportunities.  Such solution makes universal compaction behaves very similar to level compaction.

In the meantime, we also observed the following needs in level compaction:
* Allow compaction across multiple levels to reduce write amplification.

This undoubtably makes level compaction behaves similar to universal compaction.  This leads us to a global solution --- unifying level and universal compaction.

## The Goals
The goal of unifying level and universal compactions is to take the advantages from the two compactions.  Specifically, we would like to achieve the following goals:

* Reduce write amplification by considering compacting files across multiple levels.
* Reduce storage space requirement by deleting unused files during the compaction.
* Improve scalability by partitioning one big compaction work into multiple smaller compaction jobs.
* Reduce maintenance effort by unifying the two code paths into one.

## Possible Solutions
There are basically three approaches to unify universal and level compactions, where I found option 1 might be the best among the all:

1. Add universal compaction features to level compaction and deprecate universal compaction.
 * pros: Changes can be incremental, easy to maintain and test.  Can reuse existing optimizations on level compaction.
2. Add level compaction features to universal compaction and deprecate level compaction.
 * pros: Changes can be incremental, easy to maintain and test.
 * cons: Requires redo many optimizations that are already done in level compaction.
3. Create a new compaction algorithm and depreciate both level and universal compactions.
 * pros: More flexibility to do code refactor.
 * cons: Changes are likely not incremental.  Requires more effort on integrating with existing code.


## The Proposed Approach
To support universal compaction in the current level compaction, we need to enable the following features:
* Allow compaction across multiple levels.
* Allow cross-level compaction to be partitioned into multiple smaller compaction jobs to improve concurrency.
* Allow number of levels to be dynamically increase or decrease, optionally without upper-bound limit.

To allow compaction across multiple levels, we need to have a way to determine when to extend the current compaction to cover the next level, and here's my proposal:

1. Pick the largest file in input_level that is not being compacted yet
2. If all the overlapping files in input_level+1 are not being compacted, then create a new Compaction Instance encompassing all the files in input_level and input_level + 1
3. If the overlapping files in input_level+2 are not currently being compacted, then include those files from input_level+2 only when the following condition is true:
>       MIN( the size of current compaction input files, the size of overlapping files in the next level)
>     ----------------------------------------------------------------------------------------------------- >=   1 / (size multiplier between consecutive levels) 
>           the size of current compaction input files + the size of overlapping files in the next level 
>

4. If we were able to add more files via N3, then repeat N3 with the next higher level

Step 1 and 2 is the same as what we have in the current level compaction.  The different parts are step 3 and 4, where RocksDB expends the compaction to the next level when it observes a better-than-average write amplification.  This behavior is similar to what we have in universal compaction, which will minimize write amplification.

The challenge part here is: how to partition this big compaction run into multiple smaller one to run them in parallel, and I will explain how to achieve it in the later section.

To allow level compaction to do exactly the same as universal compaction, we can add an option where compactions can only be triggered when mem-table flush or adding files in L0.

### Running one big compaction run in multiple threads
TBD