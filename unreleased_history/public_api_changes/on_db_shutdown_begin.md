Added `EventListener::OnDBShutdownBegin`, a callback that fires once when a DB begins shutdown, including when RocksDB cleans up a failed `DB::Open()` attempt.
