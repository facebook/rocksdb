// tg_thread_local.h
#ifndef TG_THREAD_LOCAL_H
#define TG_THREAD_LOCAL_H

#include <string>

struct TGThreadMetadata {
    int client_id;
};

// Declare the thread-local variable
extern thread_local TGThreadMetadata myThreadMetadata;

// Function to access thread metadata
TGThreadMetadata& TG_GetThreadMetadata();

#endif
