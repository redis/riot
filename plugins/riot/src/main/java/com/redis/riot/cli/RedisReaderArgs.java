package com.redis.riot.cli;

import java.time.Duration;

import org.springframework.util.unit.DataSize;

import com.redis.riot.core.RedisReaderOptions;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;

import io.lettuce.core.ReadFrom;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisReaderArgs {

    @Option(names = "--scan-match", description = "SCAN MATCH pattern.", paramLabel = "<glob>")
    String scanMatch;

    @Option(names = "--scan-count", description = "SCAN COUNT option.", paramLabel = "<int>")
    long scanCount = RedisReaderOptions.DEFAULT_SCAN_COUNT;

    @Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
    String scanType;

    @Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int queueCapacity = RedisReaderOptions.DEFAULT_QUEUE_CAPACITY;

    @Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int threads = RedisReaderOptions.DEFAULT_THREADS;

    @Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int chunkSize = RedisReaderOptions.DEFAULT_CHUNK_SIZE;

    @Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int poolSize = RedisReaderOptions.DEFAULT_POOL_SIZE;

    @Option(names = "--read-from", description = "Which Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
    ReadFromEnum readFrom;

    @Option(names = "--mem-limit", description = "Maximum memory usage in megabytes for a key to be read. Use 0 to disable checks, use -1 to disable checks but report memory usage (default: 0).", paramLabel = "<MB>")
    int memLimit;

    @Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int memSamples = RedisReaderOptions.DEFAULT_MEMORY_USAGE_SAMPLES;

    @Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    long flushInterval = RedisReaderOptions.DEFAULT_FLUSHING_INTERVAL.toMillis();

    @Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete (default: no timeout).", paramLabel = "<ms>")
    Long idleTimeout;

    @Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    int notificationQueueCapacity = RedisReaderOptions.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    @Option(names = "--event-order", description = "Keyspace notification ordering strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    OrderingStrategy notificationOrdering = OrderingStrategy.PRIORITY;

    @ArgGroup(exclusive = false)
    KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

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
        options.setMemoryUsageLimit(DataSize.ofMegabytes(memLimit));
        options.setMemoryUsageSamples(memSamples);
        options.setNotificationQueueCapacity(notificationQueueCapacity);
        options.setOrderingStrategy(notificationOrdering);
        options.setPoolSize(poolSize);
        options.setQueueCapacity(queueCapacity);
        if (readFrom != null) {
            options.setReadFrom(readFrom.getReadFrom());
        }
        options.setScanCount(scanCount);
        options.setScanMatch(scanMatch);
        options.setScanType(scanType);
        options.setThreads(threads);
        return options;

    }

    public enum ReadFromEnum {

        MASTER(ReadFrom.MASTER),
        MASTER_PREFERRED(ReadFrom.MASTER_PREFERRED),

        UPSTREAM(ReadFrom.UPSTREAM),
        UPSTREAM_PREFERRED(ReadFrom.UPSTREAM_PREFERRED),

        REPLICA_PREFERRED(ReadFrom.REPLICA_PREFERRED),
        REPLICA(ReadFrom.REPLICA),

        LOWEST_LATENCY(ReadFrom.LOWEST_LATENCY),

        ANY(ReadFrom.ANY),
        ANY_REPLICA(ReadFrom.ANY_REPLICA);

        private final ReadFrom readFrom;

        private ReadFromEnum(ReadFrom readFrom) {
            this.readFrom = readFrom;
        }

        public ReadFrom getReadFrom() {
            return readFrom;
        }

    }

}
