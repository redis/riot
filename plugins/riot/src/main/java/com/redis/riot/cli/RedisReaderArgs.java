package com.redis.riot.cli;

import java.time.Duration;

import org.springframework.util.unit.DataSize;

import com.redis.riot.core.RedisReaderOptions;
import com.redis.spring.batch.common.DataType;

import io.lettuce.core.ReadFrom;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisReaderArgs {

	@Option(names = "--scan-match", description = "Pattern of keys to scan for (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	String scanMatch = RedisReaderOptions.DEFAULT_KEY_PATTERN;

	@Option(names = "--scan-count", description = "How many keys to read at once on each SCAN call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	long scanCount = RedisReaderOptions.DEFAULT_SCAN_COUNT;

	@Option(names = "--scan-type", description = "Type of keys to scan for: ${COMPLETION-CANDIDATES} (default: all types).", paramLabel = "<type>")
	DataType scanType;

	@Option(names = "--read-queue", description = "Max number of items that reader threads can put in the shared queue (default: ${DEFAULT-VALUE}). When the queue is full, reader threads wait for space to become available. Queue size should be at least 'threads * batch', e.g. '--read-threads 4 --read-batch 500' => '--read-queue 2000'.", paramLabel = "<int>")
	int queueCapacity = RedisReaderOptions.DEFAULT_QUEUE_CAPACITY;

	@Option(names = "--read-threads", description = "How many value reader threads to use in parallel (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int threads = RedisReaderOptions.DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of values each reader thread should read in a pipelined call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int chunkSize = RedisReaderOptions.DEFAULT_CHUNK_SIZE;

	@Option(names = "--read-pool", description = "Size of the connection pool shared by reader threads (default: ${DEFAULT-VALUE}). Can be smaller than the number of threads.", paramLabel = "<int>")
	int poolSize = RedisReaderOptions.DEFAULT_POOL_SIZE;

	@Option(names = "--read-from", description = "Which Redis cluster nodes to read from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	ReadFromEnum readFrom;

	@Option(names = "--mem-limit", description = "Maximum memory usage in megabytes for a key to be read (default: ${DEFAULT-VALUE}). Use 0 to disable checks, use -1 to disable checks but report memory usage.", paramLabel = "<MB>")
	int memLimit;

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int memSamples = RedisReaderOptions.DEFAULT_MEMORY_USAGE_SAMPLES;

	@Option(names = "--flush-interval", description = "Max duration in millis between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	long flushInterval = RedisReaderOptions.DEFAULT_FLUSH_INTERVAL.toMillis();

	@Option(names = "--idle-timeout", description = "Min number of millis to consider transfer complete (default: no timeout).", paramLabel = "<ms>")
	long idleTimeout;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	int notificationQueueCapacity = RedisReaderOptions.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	@ArgGroup(exclusive = false)
	KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

	public void setIdleTimeout(Long timeout) {
		this.idleTimeout = timeout;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public RedisReaderOptions readerOptions() {
		RedisReaderOptions options = new RedisReaderOptions();
		options.setChunkSize(chunkSize);
		options.setFlushInterval(Duration.ofMillis(flushInterval));
		if (idleTimeout > 0) {
			options.setIdleTimeout(Duration.ofMillis(idleTimeout));
		}
		options.setKeyFilterOptions(keyFilterArgs.keyFilterOptions());
		options.setMemoryUsageLimit(DataSize.ofMegabytes(memLimit));
		options.setMemoryUsageSamples(memSamples);
		options.setNotificationQueueCapacity(notificationQueueCapacity);
		options.setPoolSize(poolSize);
		options.setQueueCapacity(queueCapacity);
		if (readFrom != null) {
			options.setReadFrom(readFrom.getReadFrom());
		}
		options.setScanCount(scanCount);
		options.setKeyPattern(scanMatch);
		options.setKeyType(scanType);
		options.setThreads(threads);
		return options;

	}

	public enum ReadFromEnum {

		MASTER(ReadFrom.MASTER), MASTER_PREFERRED(ReadFrom.MASTER_PREFERRED),

		UPSTREAM(ReadFrom.UPSTREAM), UPSTREAM_PREFERRED(ReadFrom.UPSTREAM_PREFERRED),

		REPLICA_PREFERRED(ReadFrom.REPLICA_PREFERRED), REPLICA(ReadFrom.REPLICA),

		LOWEST_LATENCY(ReadFrom.LOWEST_LATENCY),

		ANY(ReadFrom.ANY), ANY_REPLICA(ReadFrom.ANY_REPLICA);

		private final ReadFrom readFrom;

		private ReadFromEnum(ReadFrom readFrom) {
			this.readFrom = readFrom;
		}

		public ReadFrom getReadFrom() {
			return readFrom;
		}

	}

}
