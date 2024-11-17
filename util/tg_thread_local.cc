#include "rocksdb/tg_thread_local.h"

thread_local TGThreadMetadata tg_thread_metadata;

TGThreadMetadata& TG_GetThreadMetadata() {
    return tg_thread_metadata;
}
