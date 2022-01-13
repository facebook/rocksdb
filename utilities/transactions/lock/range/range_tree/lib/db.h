#ifndef _DB_H
#define _DB_H

#include <stdint.h>
#include <sys/types.h>

typedef struct __toku_dbt DBT;

// port: this is currently not used
struct simple_dbt {
  uint32_t len;
  void *data;
};

// engine status info
// engine status is passed to handlerton as an array of
// TOKU_ENGINE_STATUS_ROW_S[]
typedef enum {
  STATUS_FS_STATE = 0,  // interpret as file system state (redzone) enum
  STATUS_UINT64,        // interpret as uint64_t
  STATUS_CHARSTR,       // interpret as char *
  STATUS_UNIXTIME,      // interpret as time_t
  STATUS_TOKUTIME,      // interpret as tokutime_t
  STATUS_PARCOUNT,      // interpret as PARTITIONED_COUNTER
  STATUS_DOUBLE         // interpret as double
} toku_engine_status_display_type;

typedef enum {
  TOKU_ENGINE_STATUS = (1ULL << 0),  // Include when asking for engine status
  TOKU_GLOBAL_STATUS =
      (1ULL << 1),  // Include when asking for information_schema.global_status
} toku_engine_status_include_type;

typedef struct __toku_engine_status_row {
  const char *keyname;  // info schema key, should not change across revisions
                        // without good reason
  const char
      *columnname;  // column for mysql, e.g. information_schema.global_status.
                    // TOKUDB_ will automatically be prefixed.
  const char *legend;  // the text that will appear at user interface
  toku_engine_status_display_type type;  // how to interpret the value
  toku_engine_status_include_type
      include;  // which kinds of callers should get read this row?
  union {
    double dnum;
    uint64_t num;
    const char *str;
    char datebuf[26];
    struct partitioned_counter *parcount;
  } value;
} * TOKU_ENGINE_STATUS_ROW, TOKU_ENGINE_STATUS_ROW_S;

#define DB_BUFFER_SMALL -30999
#define DB_LOCK_DEADLOCK -30995
#define DB_LOCK_NOTGRANTED -30994
#define DB_NOTFOUND -30989
#define DB_KEYEXIST -30996
#define DB_DBT_MALLOC 8
#define DB_DBT_REALLOC 64
#define DB_DBT_USERMEM 256

/* PerconaFT specific error codes */
#define TOKUDB_OUT_OF_LOCKS -100000

typedef void (*lock_wait_callback)(void *arg, uint64_t requesting_txnid,
                                   uint64_t blocking_txnid);

struct __toku_dbt {
  void *data;
  size_t size;
  size_t ulen;
  // One of DB_DBT_XXX flags
  uint32_t flags;
};

#endif
