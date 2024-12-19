Introduce `CloseOptions` to `Close()` API. If `close_options.prepare_close_fn` is set, it's called right before `DBImpl::CloseImpl()` inside DBImpl::Close()
