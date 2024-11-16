* Fix leaks of some open SST files (until `DB::Close()`) that are written but never become live due to various failures. (We now have a check for such leaks with no outstanding issues.)
