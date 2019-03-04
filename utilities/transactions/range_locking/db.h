#ifndef _DB_H
#define _DB_H

#include <sys/types.h>
#include <stdint.h>

typedef struct __toku_db DB;
typedef struct __toku_dbt DBT;
struct simple_dbt {
    uint32_t len;
    void     *data;
};

// engine status info
// engine status is passed to handlerton as an array of TOKU_ENGINE_STATUS_ROW_S[]
typedef enum {
   FS_STATE = 0,   // interpret as file system state (redzone) enum 
   UINT64,         // interpret as uint64_t 
   CHARSTR,        // interpret as char * 
   UNIXTIME,       // interpret as time_t 
   TOKUTIME,       // interpret as tokutime_t 
   PARCOUNT,       // interpret as PARTITIONED_COUNTER
   DOUBLE          // interpret as double
} toku_engine_status_display_type;

typedef enum {
   TOKU_ENGINE_STATUS             = (1ULL<<0),  // Include when asking for engine status
   TOKU_GLOBAL_STATUS = (1ULL<<1),  // Include when asking for information_schema.global_status
} toku_engine_status_include_type; 

typedef struct __toku_engine_status_row {
  const char * keyname;                  // info schema key, should not change across revisions without good reason 
  const char * columnname;               // column for mysql, e.g. information_schema.global_status. TOKUDB_ will automatically be prefixed.
  const char * legend;                   // the text that will appear at user interface 
  toku_engine_status_display_type type;  // how to interpret the value 
  toku_engine_status_include_type include;  // which kinds of callers should get read this row?
  union {              
         double   dnum; 
         uint64_t num; 
         const char *   str; 
         char           datebuf[26]; 
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

typedef struct {
    uint32_t capacity;
    uint32_t size;
    DBT *dbts;
} DBT_ARRAY;

DBT_ARRAY * toku_dbt_array_init(DBT_ARRAY *dbts, uint32_t size) __attribute__((__visibility__("default")));
void toku_dbt_array_destroy(DBT_ARRAY *dbts) __attribute__((__visibility__("default")));
void toku_dbt_array_destroy_shallow(DBT_ARRAY *dbts) __attribute__((__visibility__("default")));
void toku_dbt_array_resize(DBT_ARRAY *dbts, uint32_t size) __attribute__((__visibility__("default")));

typedef void (*lock_wait_callback)(void *arg, uint64_t requesting_txnid, uint64_t blocking_txnid);

struct __toku_dbt {
  void*data;
  uint32_t size;
  uint32_t ulen;
  uint32_t flags;
};
typedef struct __toku_descriptor {
    DBT       dbt;
} *DESCRIPTOR, DESCRIPTOR_S;

struct __toku_db {
  DESCRIPTOR cmp_descriptor /* saved row/dictionary descriptor for aiding in comparisons */;
};
#endif
