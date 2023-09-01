package com.redis.riot.cli;

import java.time.Duration;

import org.springframework.util.unit.DataSize;

import com.redis.riot.core.RedisReaderOptions;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisReaderArgs {

    @Option(names = "--scan-match", description = "SCAN MATCH pattern.", paramLabel = "<glob>")
    private String scanMatch;

    @Option(names = "--scan-count", description = "SCAN COUNT option.", paramLabel = "<int>")
    private Long scanCount;

    @Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
    private String scanType;

    @Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int queueCapacity = RedisReaderOptions.DEFAULT_QUEUE_CAPACITY;

    @Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = RedisReaderOptions.DEFAULT_THREADS;

    @Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int chunkSize = RedisReaderOptions.DEFAULT_CHUNK_SIZE;

    @Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolSize = RedisReaderOptions.DEFAULT_POOL_SIZE;

    @Option(names = "--read-from", description = "Which Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
    private ReadFromEnum readFrom;

    @Option(names = "--mem-limit", description = "Maximum memory usage in MB for a key to be read. Use 0 to disable memory usage checks (default: 0).", paramLabel = "<MB>")
    private DataSize memLimit;

    @Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int memSamples = RedisReaderOptions.DEFAULT_MEMORY_USAGE_SAMPLES;

    @Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    private long flushInterval = RedisReaderOptions.DEFAULT_FLUSHING_INTERVAL.toMillis();

    @Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete (default: no timeout).", paramLabel = "<ms>")
    private Long idleTimeout;

    @Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int notificationQueueCapacity = RedisReaderOptions.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    @Option(names = "--event-order", description = "Keyspace notification ordering strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private OrderingStrategy notificationOrdering = OrderingStrategy.PRIORITY;

    @ArgGroup(exclusive = false)
    private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

    public void setIdleTimeout(Long timeout) {
        this.idleTimeout = timeout;
    }

    public void setNotificationQueueCapacity(int capacity) {
        this.notificationQueueCapacity = capacity;
    }

    public RedisReaderOptions redisReaderOptions() {
        RedisReaderOptions options = new RedisReaderOptions();
        options.setChunkSize(chunkSize);
        options.setFlushingInterval(Duration.ofMillis(flushInterval));
        options.setKeyFilterOptions(keyFilterArgs.keyFilterOptions());
        if (idleTimeout != null) {
            options.setIdleTimeout(Duration.ofMillis(idleTimeout));
        }
        options.setMemoryUsageLimit(memLimit);
        options.setMemoryUsageSamples(memSamples);
        options.setNotificationQueueCapacity(notificationQueueCapacity);
        options.setOrderingStrategy(notificationOrdering);
        options.setPoolSize(poolSize);
        options.setQueueCapacity(queueCapacity);
        if (readFrom != null) {
            options.setReadFrom(readFrom.getValue());
        }
        options.setScanCount(scanCount);
        options.setScanMatch(scanMatch);
        options.setScanType(scanType);
        options.setThreads(threads);
        return options;

    }

}
