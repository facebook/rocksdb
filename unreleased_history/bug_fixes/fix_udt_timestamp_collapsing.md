Bugfix for UDT timestamp collapsing. Switch from always using 0 as minimum timestamp to get minimum timestamp from comparator, so that signed timestamp type works correctly.
