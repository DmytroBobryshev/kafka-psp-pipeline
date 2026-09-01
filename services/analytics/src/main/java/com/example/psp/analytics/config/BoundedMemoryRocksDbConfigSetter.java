package com.example.psp.analytics.config;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

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
    }
}
