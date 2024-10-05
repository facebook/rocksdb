* `GetApproximateMemTableStats()` could return disastrously bad estimates 5-25% of the time. The function has been re-engineered to return much better estimates with similar CPU cost.
