## Introduction
Level compaction style is the default compaction style of RocksDB, so it is also the most widely used compaction style among users. Sometimes users are curious that how level compaction chooses which files to be compacted in each compaction. In this wiki, we will elaborate more on this topic to save your time in reading code :smirk:.

## Steps
1. Go from level 0 to highest level to pick the level, L<sub>b</sub>, that the score of this level is the largest and is larger than 1 as the compaction base level.
2. Determine the compaction output level L<sub>o</sub> = L<sub>b</sub> + 1
3. According to different [compaction priority options](http://rocksdb.org/blog/2016/01/29/compaction_pri.html), find the first file that should be compacted with the highest priority. If the file or its parents on L<sub>o</sub>(The files the key ranges of which overlap) are being compacted by another compaction job, skip to the file with the secondary highest priority until find **one** candidate file. Adds this file into compaction **inputs**.
4. Keep expanding inputs until we are sure that there is a "clean cut" boundary between the files in input and the surrounding files. This will ensure that no parts of a key are lost during compaction. For example, we have five files with key range:
   <pre>
       f1[a1 a2] f2[a3 a4] f3[a4 a6] f4[a6 a7] f5[a8 a9]
   </pre>
   
   If we choose f3 in step 3, then in step 4, we have to expand inputs from {f3} to {f2, f3, f4} because the boundary of f2 and f3 are continuous, as are f3 and f4. The reason that two files may share the same user key boundary is that RocksDB store InternalKey in files which consists of user key, sequence number of key type. So files may store multiple InternalKeys with the same user key. Therefore, if compaction happens, all the InternalKeys with the same user key have to be compacted altogether.
5. Check that the files in current **inputs** don't overlap with any files being compacted. Otherwise, try to find any manual compaction available. If not, abort this compaction pick job.
6. Find all the files on L<sub>o</sub> that overlap with the files in **inputs** and expand them as the same way in step 4 until we find a "clean cut" of files on L<sub>o</sub>. If anyone of these files are being compacted, abort this compaction pick job. Otherwise, put them into **output_level_inputs**.
7. An optional optimization step. See if we can further grow the number of inputs on L<sub>b</sub> without changing the number of L<sub>o</sub> files we pick up. We also choose NOT to expand if this would cause L<sub>b</sub> to include InternalKey for some user key, while excluding other InternalKey for the same user key, i.e., "a non-clean cut". This can happen when one user key spans multiple files. The previous sentence may be confusing by words, so here I give an example to illustrate this expansion optimization.

    Consider such a case:
    <pre>
    L<sub>b</sub>: f1[B E] f2[F G] f3[H I] f4[J M]
    L<sub>o</sub>: f5[A C] f6[D K] f7[L O]
    </pre>

    If we initially pick f2 in step 3, now we will compact f2(**inputs**) and f6(**output_level_inputs** in step 4). But we can safely compact f2, f3 and f6 without expanding the output level.
8. The files in **inputs** and **output_level_inputs** are the candidate files for this level compaction.

Cheers!:beers::beers::beers: