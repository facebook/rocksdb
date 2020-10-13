// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.test;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.RemoveEmptyValueCompactionFilter;

/**
 * Simple CompactionFilterFactory class used in tests. Generates RemoveEmptyValueCompactionFilters.
 */
public class RemoveEmptyValueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveEmptyValueCompactionFilter> {
    @Override
    public RemoveEmptyValueCompactionFilter createCompactionFilter(final AbstractCompactionFilter.Context context) {
        return new RemoveEmptyValueCompactionFilter();
    }

    @Override
    public String name() {
        return "RemoveEmptyValueCompactionFilterFactory";
    }
}
