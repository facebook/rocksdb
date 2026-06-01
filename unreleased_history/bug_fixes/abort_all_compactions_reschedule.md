Fixed a bug where `AbortAllCompactions()` could leave automatic compaction work unscheduled when aborting before the background worker picked it, causing compaction and write stalls until DB restart.
