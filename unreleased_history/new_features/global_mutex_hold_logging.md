Added opt-in sampled logging of long-held DB-wide mutexes under `StatsLevel::kAll`, with a runtime-changeable `mutex_hold_log_threshold_us` DB option defaulting to 0 (disabled).
