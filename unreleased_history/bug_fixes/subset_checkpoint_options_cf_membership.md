Fixed subset checkpoints so their newest OPTIONS file lists only the column
families retained by the checkpoint. This applies with both legacy
highest-numbered OPTIONS selection and MANIFEST-tracked OPTIONS selection, and
prevents `LoadLatestOptions` from recreating column families that the subset
checkpoint intentionally dropped from its copied MANIFEST.
