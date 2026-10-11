Added the experimental `DBOptions::track_options_file_number_in_manifest`
rollout option (with C and Java bindings). The option is persisted in each
OPTIONS file. When enabled, RocksDB durably creates an OPTIONS file and then
commits its file number in MANIFEST. Column-family create/drop commits the
configuration snapshot and membership in one MANIFEST record. A newer tracked
OPTIONS file without a commit is ignored after a crash, while a newer OPTIONS
file written with tracking disabled retains the legacy highest-numbered-file
behavior. Batched column-family changes publish one snapshot; after partial
success it can be a safe superset that persisted-options APIs filter against
live MANIFEST membership. Ordinary `DB::Open` continues to use caller-supplied
options and does not depend on the operational snapshot.
