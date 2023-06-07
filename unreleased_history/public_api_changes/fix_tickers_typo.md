Some tickers (namely, `ERROR_HANDLER_BG_ERROR_COUNT`, `ERROR_HANDLER_BG_IO_ERROR_COUNT`, and `ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT`)
used to have misspelled printable names (`errro` instead of `error`).
They were cloned under fixed names, keeping the old ones as well for backward compatibility (the corresponding enumerators have the `_MISSPELLED` suffix).
The misspelled tickers are intended to be removed in a future major release.
