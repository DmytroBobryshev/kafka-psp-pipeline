package com.example.psp.analytics.config;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

/**
 * Caps RocksDB's per-store memory so a laptop can run this topology (M10).
 *
 * <p>Kafka Streams gives <b>every task</b> its own RocksDB instance per store - not one per
 * store, one per store <i>per partition</i>. This application has 12 input partitions and one
 * materialised window store, so 12 RocksDB instances exist regardless of
 * {@code num.stream.threads} (threads decide how many run concurrently, not how many exist).
 * RocksDB's defaults are sized for a dedicated server: a 50 MiB block cache and 3 x 64 MiB
 * memtables per instance would reserve on the order of 2.9 GiB of off-heap memory for a workload
 * whose entire state is a few hundred counters, and would flush oversized SST files onto a disk
 * this repo's README says has ~36 GiB spare.
 *
 * <p>The numbers below are deliberately small - 4 MiB of block cache and 2 x 2 MiB memtables per
 * instance, ~96 MiB total across 12 tasks. They are correct for this workload precisely because
 * the working set is tiny: a 1-minute window over a handful of merchants. On a real deployment
 * these are the wrong numbers and the right mechanism, which is why this class exists at all
 * rather than the values being pasted into {@code application.yml} - {@code rocksdb.config.setter}
 * is the only way to reach these settings.
 *
 * <p>Must be public with a public no-arg constructor: Streams instantiates it reflectively, once
 * per store instance.
 */
public class BoundedMemoryRocksDbConfigSetter implements RocksDBConfigSetter {

    private static final long BLOCK_CACHE_BYTES = 4L * 1024 * 1024;
    private static final long WRITE_BUFFER_BYTES = 2L * 1024 * 1024;
    private static final int MAX_WRITE_BUFFERS = 2;

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_BYTES);
        options.setTableFormatConfig(tableConfig);

        // Memtable: how much is buffered in memory before an SST file is written.
        options.setWriteBufferSize(WRITE_BUFFER_BYTES);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
    }

    @Override
    public void close(String storeName, Options options) {
        // Nothing to close: no Cache/WriteBufferManager instance is retained here (a shared,
        // explicitly-constructed Cache would have to be closed exactly once, which is the usual
        // reason this method is not empty).
    }
}
