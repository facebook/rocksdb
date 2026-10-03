Custom index blocks now reuse cached contents on SST reopen, and deprecated UDI boolean options can be cleared without overriding an explicitly configured index mode.

Legacy UDI options copied from a factory can now be cleared before constructing a new factory without retaining the derived mode. The C API preserves both that rollback behavior and explicitly selected modes. OPTIONS files serialize equivalent legacy booleans for `kStandardDefault`, `kStandardRequired`, and `kCustomDefault` so older readers retain routing.

Empty custom-index output retains the standard-index fallback in modes that write both indexes; custom-only output must remain nonempty. Default primary reads retain the table factory's precedence over a legacy per-read factory pointer, while explicit custom selectors still check its name.
