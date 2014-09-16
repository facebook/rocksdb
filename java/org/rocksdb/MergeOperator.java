// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.*;

/**
 * MergeOperator holds an operator to be applied when compacting
 * two values held under the same key in order to obtain a single
 * value.
 */
public abstract class MergeOperator {

    abstract protected long newMergeOperatorHandle();

}
