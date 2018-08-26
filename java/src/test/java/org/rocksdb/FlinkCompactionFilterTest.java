/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.FlinkCompactionFilter.StateType;
import org.rocksdb.FlinkCompactionFilter.TimeProvider;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkCompactionFilterTest {
    private static final String MERGE_OPERATOR_NAME = "stringappendtest";
    private static final byte DELIMITER = ',';
    private static final long TTL = 100;

    private TestTimeProvider timeProvider;
    private List<StateContext> stateContexts;
    private List<ColumnFamilyDescriptor> cfDescs;
    private List<ColumnFamilyHandle> cfHandles;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Before
    public void init() {
        timeProvider = new TestTimeProvider();
        stateContexts = new ArrayList<>(StateType.values().length);
        cfDescs = new ArrayList<>();
        cfHandles = new ArrayList<>();
        cfDescs.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (StateType type : StateType.values()) {
            StateContext stateContext = StateContext.create(type, timeProvider);
            stateContexts.add(stateContext);
            cfDescs.add(stateContext.getCfDesc());
        }
    }

    @After
    public void cleanup() {
        for (StateContext stateContext : stateContexts) {
            stateContext.cfDesc.getOptions().close();
            stateContext.filter.close();
        }
    }

    @Test
    public void checkStateTypeEnumOrder() {
        // if the order changes it also needs to be adjusted
        // in utilities/flink/flink_compaction_filter.h
        // and in utilities/flink/flink_compaction_filter_test.cc
        assertThat(StateType.Value.ordinal()).isEqualTo(0);
        assertThat(StateType.List.ordinal()).isEqualTo(1);
        assertThat(StateType.Map.ordinal()).isEqualTo(2);
    }

    @Test
    public void testCompactionFilter() throws RocksDBException {
        try(DBOptions options = createDbOptions();
            RocksDB rocksDb = setupDb(options)) {
            try {
                for (StateContext stateContext : stateContexts) {
                    stateContext.updateValueWithTimestamp(rocksDb);
                    assertThat(stateContext.getValueWithTimestamp(rocksDb)).isEqualTo(stateContext.unexpiredValue());
                    rocksDb.compactRange(stateContext.columnFamilyHandle);
                    assertThat(stateContext.getValueWithTimestamp(rocksDb)).isEqualTo(stateContext.unexpiredValue());
                }

                timeProvider.expire();

                for (StateContext stateContext : stateContexts) {
                    assertThat(stateContext.getValueWithTimestamp(rocksDb)).isEqualTo(stateContext.unexpiredValue());
                    rocksDb.compactRange(stateContext.columnFamilyHandle);
                    assertThat(stateContext.getValueWithTimestamp(rocksDb)).isEqualTo(stateContext.expiredValue());
                    rocksDb.compactRange(stateContext.columnFamilyHandle);
                }
            } finally {
                for (ColumnFamilyHandle cfHandle : cfHandles) {
                    cfHandle.close();
                }
            }
        }
    }

    private static DBOptions createDbOptions() {
        return new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
    }

    private RocksDB setupDb(DBOptions options) throws RocksDBException {
        RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(), cfDescs, cfHandles);
        for (int i = 0; i < stateContexts.size(); i++) {
            stateContexts.get(i).columnFamilyHandle = cfHandles.get(i + 1);
        }
        return db;
    }

    private static class TestTimeProvider implements TimeProvider {
        long time = 0;

        @Override
        public long currentTimestamp() {
            return time;
        }

        void expire() {
            time += TTL + TTL / 2;
        }
    }

    private static class StateContext {
        private static final int LONG_LENGTH = 8;

        private final String cf;
        final String key;
        final ColumnFamilyDescriptor cfDesc;
        final String userValue;
        final long currentTime;
        final FlinkCompactionFilter filter;

        ColumnFamilyHandle columnFamilyHandle;

        static StateContext create(StateType type, TimeProvider timeProvider) {
            if (type == StateType.Map) {
                return new MapStateContext(timeProvider);
            } else if (type == StateType.List) {
                return new ListStateContext(timeProvider);
            } else {
                return new StateContext(type, timeProvider);
            }
        }

        private StateContext(StateType type, TimeProvider timeProvider) {
            this.currentTime = timeProvider.currentTimestamp();
            userValue = type.name() + "StateValue";
            cf = type.name() + "StateCf";
            key = type.name() + "StateKey";
            filter = new FlinkCompactionFilter(type, TTL, timeProvider);
            cfDesc = new ColumnFamilyDescriptor(getASCII(cf), getOptionsWithFilter(filter));
        }

        private static ColumnFamilyOptions getOptionsWithFilter(FlinkCompactionFilter filter) {
            return new ColumnFamilyOptions()
                    .setCompactionFilter(filter)
                    .setMergeOperatorName(MERGE_OPERATOR_NAME);
        }

        private static byte[] getASCII(String str) {
            return str.getBytes(StandardCharsets.US_ASCII);
        }

        public String getKey() {
            return key;
        }

        ColumnFamilyDescriptor getCfDesc() {
            return cfDesc;
        }

        byte[] getValueWithTimestamp(RocksDB db) throws RocksDBException {
            return db.get(columnFamilyHandle, getASCII(key));
        }

        void updateValueWithTimestamp(RocksDB db) throws RocksDBException {
            db.put(columnFamilyHandle, getASCII(key), valueWithTimestamp());
        }

        byte[] valueWithTimestamp() {
            return valueWithTimestamp(0);
        }

        byte[] valueWithTimestamp(int offset) {
            return valueWithTimestamp(offset, currentTime);
        }

        byte[] valueWithTimestamp(int offset, long timestamp) {
            ByteBuffer buffer = getByteBuffer(offset);
            buffer.put(new byte[offset]);
            appendValueWithTimestamp(buffer, userValue, timestamp);
            return buffer.array();
        }

        ByteBuffer getByteBuffer(int offset) {
            int length = offset + LONG_LENGTH + userValue.length();
            return ByteBuffer.allocate(length);
        }

        void appendValueWithTimestamp(ByteBuffer buffer, String value, long timestamp) {
            buffer.putLong(timestamp);
            buffer.put(getASCII(value));
        }

        byte[] unexpiredValue() {
            return valueWithTimestamp();
        }

        byte[] expiredValue() {
            return null;
        }

        private static class MapStateContext extends StateContext {
            private static final int MAP_NULL_VALUE_OFFSET = 1;

            private MapStateContext(TimeProvider timeProvider) {
                super(StateType.Map, timeProvider);
            }

            @Override
            public byte[] valueWithTimestamp() {
                return valueWithTimestamp(MAP_NULL_VALUE_OFFSET);
            }
        }

        private static class ListStateContext extends StateContext {
            private ListStateContext(TimeProvider timeProvider) {
                super(StateType.List, timeProvider);
            }

            @Override
            void updateValueWithTimestamp(RocksDB db) throws RocksDBException {
                db.merge(columnFamilyHandle, getASCII(key), list(2, currentTime));
                db.merge(columnFamilyHandle, getASCII(key), list(2, currentTime + TTL));
                db.merge(columnFamilyHandle, getASCII(key), valueWithTimestamp());
                db.merge(columnFamilyHandle, getASCII(key), valueWithTimestamp(0, currentTime + TTL));
                db.merge(columnFamilyHandle, getASCII(key), valueWithTimestamp());
                db.merge(columnFamilyHandle, getASCII(key), valueWithTimestamp(0, currentTime + TTL));
            }

            @Override
            byte[] unexpiredValue() {
                ByteBuffer buffer = getByteBufferForList(8);
                appendValueWithTimestamp(buffer, userValue, currentTime);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime + TTL);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime + TTL);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime + TTL);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime);
                buffer.put(DELIMITER);
                appendValueWithTimestamp(buffer, userValue, currentTime + TTL);
                return buffer.array();
            }

            @Override
            byte[] expiredValue() {
                return list(4, currentTime + TTL);
            }

            private byte[] list(int numberOfElements, long timestamp) {
                ByteBuffer buffer = getByteBufferForList(numberOfElements);
                for (int i = 0; i < numberOfElements; i++) {
                    appendValueWithTimestamp(buffer, userValue, timestamp);
                    if (i < numberOfElements - 1) {
                        buffer.put(DELIMITER);
                    }
                }
                return buffer.array();
            }

            private ByteBuffer getByteBufferForList(int numberOfElements) {
                int length = ((LONG_LENGTH + userValue.length() + 1) * numberOfElements) - 1;
                return ByteBuffer.allocate(length);
            }
        }
    }

}