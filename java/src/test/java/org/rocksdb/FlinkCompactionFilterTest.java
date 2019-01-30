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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkCompactionFilterTest {
    private static final int LONG_LENGTH = 8;
    private static final int INT_LENGTH = 4;
    private static final String MERGE_OPERATOR_NAME = "stringappendtest";
    private static final byte DELIMITER = ',';
    private static final long TTL = 100;
    private static final long QUERY_TIME_AFTER_NUM_ENTRIES = 100;
    private static final int TEST_TIMESTAMP_OFFSET = 2;
    private static final Random rnd = new Random();

    private TestTimeProvider timeProvider;
    private List<StateContext> stateContexts;
    private List<ColumnFamilyDescriptor> cfDescs;
    private List<ColumnFamilyHandle> cfHandles;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Before
    public void init() {
        timeProvider = new TestTimeProvider();
        timeProvider.time = rnd.nextLong();
        stateContexts = Arrays.asList(
                new StateContext(StateType.Value, timeProvider, TEST_TIMESTAMP_OFFSET),
                new FixedElementListStateContext(timeProvider),
                new NonFixedElementListStateContext(timeProvider)
        );
        cfDescs = new ArrayList<>();
        cfHandles = new ArrayList<>();
        cfDescs.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (StateContext stateContext : stateContexts) {
            cfDescs.add(stateContext.getCfDesc());
        }
    }

    @After
    public void cleanup() {
        for (StateContext stateContext : stateContexts) {
            stateContext.cfDesc.getOptions().close();
            stateContext.filterFactory.close();
        }
    }

    @Test
    public void checkStateTypeEnumOrder() {
        // if the order changes it also needs to be adjusted
        // in utilities/flink/flink_compaction_filter.h
        // and in utilities/flink/flink_compaction_filter_test.cc
        assertThat(StateType.Disabled.ordinal()).isEqualTo(0);
        assertThat(StateType.Value.ordinal()).isEqualTo(1);
        assertThat(StateType.List.ordinal()).isEqualTo(2);
    }

    @Test
    public void testCompactionFilter() throws RocksDBException {
        try(DBOptions options = createDbOptions();
            RocksDB rocksDb = setupDb(options)) {
            try {
                for (StateContext stateContext : stateContexts) {
                    stateContext.updateValueWithTimestamp(rocksDb);
                    stateContext.checkUnexpired(rocksDb);
                    rocksDb.compactRange(stateContext.columnFamilyHandle);
                    stateContext.checkUnexpired(rocksDb);
                }

                timeProvider.time += TTL + TTL / 2; // expire state

                for (StateContext stateContext : stateContexts) {
                    stateContext.checkUnexpired(rocksDb);
                    rocksDb.compactRange(stateContext.columnFamilyHandle);
                    stateContext.checkExpired(rocksDb);
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

    private static class StateContext {
        private final String cf;
        final String key;
        final ColumnFamilyDescriptor cfDesc;
        final String userValue;
        final long currentTime;
        final FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory;

        ColumnFamilyHandle columnFamilyHandle;

        private StateContext(StateType type, TimeProvider timeProvider, int timestampOffset) {
            this.currentTime = timeProvider.currentTimestamp();
            userValue = type.name() + "StateValue";
            cf = getClass().getSimpleName() + "StateCf";
            key = type.name() + "StateKey";
            filterFactory = new FlinkCompactionFilter.FlinkCompactionFilterFactory(timeProvider, createLogger());
            filterFactory.configure(createConfig(type, timestampOffset));
            cfDesc = new ColumnFamilyDescriptor(getASCII(cf), getOptionsWithFilter(filterFactory));
        }

        private Logger createLogger() {
            try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
                return new Logger(opts) {
                    @Override
                    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                        System.out.println(infoLogLevel + ": " + logMsg);
                    }
                };
            }
        }

        FlinkCompactionFilter.Config createConfig(StateType type, int timestampOffset) {
            return FlinkCompactionFilter.Config.createNotList(type, timestampOffset, TTL, QUERY_TIME_AFTER_NUM_ENTRIES);
        }

        private static ColumnFamilyOptions getOptionsWithFilter(
                FlinkCompactionFilter.FlinkCompactionFilterFactory filterFactory) {
            return new ColumnFamilyOptions()
                    .setCompactionFilterFactory(filterFactory)
                    .setMergeOperatorName(MERGE_OPERATOR_NAME);
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
            return valueWithTimestamp(TEST_TIMESTAMP_OFFSET);
        }

        byte[] valueWithTimestamp(@SuppressWarnings("SameParameterValue") int offset) {
            return valueWithTimestamp(offset, currentTime);
        }

        byte[] valueWithTimestamp(int offset, long timestamp) {
            ByteBuffer buffer = getByteBuffer(offset);
            buffer.put(new byte[offset]);
            appendValueWithTimestamp(buffer, userValue, timestamp);
            return buffer.array();
        }

        void appendValueWithTimestamp(ByteBuffer buffer, String value, long timestamp) {
            buffer.putLong(timestamp);
            buffer.putInt(value.length());
            buffer.put(getASCII(value));
        }

        ByteBuffer getByteBuffer(int offset) {
            int length = offset + LONG_LENGTH + INT_LENGTH + userValue.length();
            return ByteBuffer.allocate(length);
        }

        byte[] unexpiredValue() {
            return valueWithTimestamp();
        }

        byte[] expiredValue() {
            return null;
        }

        void checkUnexpired(RocksDB db) throws RocksDBException {
            assertThat(getValueWithTimestamp(db)).isEqualTo(unexpiredValue());
        }

        void checkExpired(RocksDB db) throws RocksDBException {
            assertThat(getValueWithTimestamp(db)).isEqualTo(expiredValue());
        }
    }

    private static class FixedElementListStateContext extends StateContext {
        private FixedElementListStateContext(TimeProvider timeProvider) {
            super(StateType.List, timeProvider,0);
        }

        @Override
        FlinkCompactionFilter.Config createConfig(StateType type, int timestampOffset) {
            //return FlinkCompactionFilter.Config.createForList(TTL, QUERY_TIME_AFTER_NUM_ENTRIES, ELEM_FILTER_FACTORY);
            return FlinkCompactionFilter.Config.createForFixedElementList(TTL, QUERY_TIME_AFTER_NUM_ENTRIES, 13 + userValue.getBytes().length);
        }

        @Override
        void updateValueWithTimestamp(RocksDB db) throws RocksDBException {
            db.merge(columnFamilyHandle, getASCII(key), listExpired(3));
            db.merge(columnFamilyHandle, getASCII(key), mixedList(2, 3));
            db.merge(columnFamilyHandle, getASCII(key), listUnexpired(4));
        }

        @Override
        byte[] unexpiredValue() {
            return mixedList(5, 7);
        }

        byte[] mergeBytes(byte[] ... bytes) {
            int length = 0;
            for (byte[] a : bytes) {
                length += a.length;
            }
            ByteBuffer buffer = ByteBuffer.allocate(length);
            for (byte[] a : bytes) {
                buffer.put(a);
            }
            return buffer.array();
        }

        @Override
        byte[] expiredValue() {
            return listUnexpired(7);
        }

        private byte[] mixedList(int numberOfExpiredElements, int numberOfUnexpiredElements) {
            assert numberOfExpiredElements > 0;
            assert numberOfUnexpiredElements > 0;
            return mergeBytes(
                    listExpired(numberOfExpiredElements),
                    new byte[] {DELIMITER},
                    listUnexpired(numberOfUnexpiredElements));
        }

        private byte[] listExpired(int numberOfElements) {
            return list(numberOfElements, currentTime);
        }

        private byte[] listUnexpired(int numberOfElements) {
            return list(numberOfElements, currentTime + TTL);
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
            int length = ((LONG_LENGTH + INT_LENGTH + userValue.length() + 1) * numberOfElements) - 1;
            return ByteBuffer.allocate(length);
        }
    }

    private static class NonFixedElementListStateContext extends FixedElementListStateContext {
        private static FlinkCompactionFilter.ListElementFilterFactory ELEM_FILTER_FACTORY = new ListElementFilterFactory();

        private NonFixedElementListStateContext(TimeProvider timeProvider) {
            super(timeProvider);
        }

        @Override
        FlinkCompactionFilter.Config createConfig(StateType type, int timestampOffset) {
            //return FlinkCompactionFilter.Config.createForList(TTL, QUERY_TIME_AFTER_NUM_ENTRIES, ELEM_FILTER_FACTORY);
            return FlinkCompactionFilter.Config.createForList(TTL, QUERY_TIME_AFTER_NUM_ENTRIES, ELEM_FILTER_FACTORY);
        }

        private static class ListElementFilterFactory implements FlinkCompactionFilter.ListElementFilterFactory {
            @Override
            public FlinkCompactionFilter.ListElementFilter createListElementFilter() {
                return new FlinkCompactionFilter.ListElementFilter() {
                    @Override
                    public int nextUnexpiredOffset(byte[] list, long ttl, long currentTimestamp) {
                        int currentOffset = 0;
                        while (currentOffset < list.length) {
                            ByteBuffer bf = ByteBuffer
                                    .wrap(list, currentOffset, list.length - currentOffset);
                            long timestamp = bf.getLong();
                            if (timestamp + ttl > currentTimestamp) {
                                break;
                            }
                            int elemLen = bf.getInt(8);
                            currentOffset += 13 + elemLen;
                        }
                        return currentOffset;
                    }
                };
            }
        }
    }

    private static byte[] getASCII(String str) {
        return str.getBytes(StandardCharsets.US_ASCII);
    }

    private static class TestTimeProvider implements TimeProvider {
        private long time;

        @Override
        public long currentTimestamp() {
            return time;
        }
    }
}