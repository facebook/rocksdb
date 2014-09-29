%module rocksby

%include <std_string.i>

%{
#include <cstdio>
#include <iostream>
#include <stdlib.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "rocksby.h"
%}

%include "rocksby.h"
