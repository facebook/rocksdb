## Rocksdb can be opened with Time to Live(TTL) support

### USE-CASES
This API should be used to open the db when key-values inserted are meant to be removed from the db in a non-strict 'ttl' amount of time therefore, this guarantees that key-values inserted will remain in the db for at least ttl amount of time and the db will make efforts to remove the key-values as soon as possible after ttl seconds of their insertion.

### BEHAVIOUR
* TTL is accepted in seconds
* (int32_t)Timestamp(creation) is suffixed to values in Put internally
* Expired TTL values are deleted in compaction only:(Timestamp+ttl<time_now)
* Get/Iterator may return expired entries(compaction not run on them yet)
* Different TTL may be used during different Opens
* Example: Open1 at t=0 with ttl=4 and insert k1,k2, close at t=2.
           Open2 at t=3 with ttl=5. Now k1,k2 should be deleted at t>=5
* read_only=true opens in the usual read-only mode. Compactions will not be triggered(neither manual nor automatic), so no expired entries removed

### CONSTRAINTS
Not specifying/passing or non-positive TTL behaves like TTL = infinity

### !!!WARNING!!!
* Calling DB::Open directly to re-open a db created by this API will get corrupt values(timestamp suffixed) and no ttl effect will be there during the second Open, so use this API consistently to open the db 
* Be careful when passing ttl with a small positive value because the whole database may be deleted in a small amount of time
    
### API
Defined in header <rocksdb/utilities/db_ttl.h>


```cpp 
static Status DBWithTTL::Open(const Options& options, const std::string& name, StackableDB** dbptr, 
                              int32_t ttl = 0, bool read_only = false);
```