package com.redis.riot.cli;

import org.springframework.util.unit.DataSize;

import com.redis.riot.core.RedisReaderOptions;

import io.lettuce.core.ReadFrom;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisReaderArgs {

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

	@Option(names = "--scan-match", description = "Pattern of keys to scan for (default: *).", paramLabel = "<glob>")
	private String scanMatch;

	@Option(names = "--scan-count", description = "How many keys to read at once on each SCAN call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = RedisReaderOptions.DEFAULT_SCAN_COUNT;

	@Option(names = "--scan-type", description = "Type of keys to scan for (default: all types).", paramLabel = "<type>")
	private String scanType;

	@Option(names = "--read-queue", description = {
			"Max number of items that reader threads can put in the shared queue (default: ${DEFAULT-VALUE}).",
			"When the queue is full, reader threads wait for space to become available.",
			"Queue size should be at least 'threads * batch', e.g. '--read-threads 4 --read-batch 500' => '--read-queue 2000'." }, paramLabel = "<int>")
	private int queueCapacity = RedisReaderOptions.DEFAULT_QUEUE_CAPACITY;

	@Option(names = "--read-threads", description = "How many value reader threads to use in parallel (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = RedisReaderOptions.DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of values each reader thread should read in a pipelined call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int chunkSize = RedisReaderOptions.DEFAULT_CHUNK_SIZE;

	@Option(names = "--read-pool", description = "Size of the connection pool shared by reader threads (default: ${DEFAULT-VALUE}). Can be smaller than the number of threads.", paramLabel = "<int>")
	private int poolSize = RedisReaderOptions.DEFAULT_POOL_SIZE;

	@Option(names = "--read-from", description = "Which Redis cluster nodes to read from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	private ReadFromEnum readFrom;

	@Option(names = "--mem-limit", description = "Maximum memory usage in megabytes for a key to be read (default: ${DEFAULT-VALUE}). Use 0 to disable checks, use -1 to disable checks but report memory usage.", paramLabel = "<MB>")
	private int memLimit;

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int memSamples = RedisReaderOptions.DEFAULT_MEMORY_USAGE_SAMPLES;

	@ArgGroup(exclusive = false)
	private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

	public RedisReaderOptions redisReaderOptions() {
		RedisReaderOptions options = new RedisReaderOptions();
		options.setChunkSize(chunkSize);
		options.setMemoryUsageLimit(DataSize.ofMegabytes(memLimit));
		options.setMemoryUsageSamples(memSamples);
		options.setPoolSize(poolSize);
		options.setQueueCapacity(queueCapacity);
		if (readFrom != null) {
			options.setReadFrom(readFrom.getReadFrom());
		}
		options.setScanCount(scanCount);
		options.setKeyPattern(scanMatch);
		options.setKeyType(scanType);
		options.setThreads(threads);
		options.setKeyFilterOptions(keyFilterArgs.keyFilterOptions());
		return options;
	}

	public String getScanMatch() {
		return scanMatch;
	}

	public void setScanMatch(String scanMatch) {
		this.scanMatch = scanMatch;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long scanCount) {
		this.scanCount = scanCount;
	}

	public String getScanType() {
		return scanType;
	}

	public void setScanType(String scanType) {
		this.scanType = scanType;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public ReadFromEnum getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFromEnum readFrom) {
		this.readFrom = readFrom;
	}

	public int getMemLimit() {
		return memLimit;
	}

	public void setMemLimit(int memLimit) {
		this.memLimit = memLimit;
	}

	public int getMemSamples() {
		return memSamples;
	}

	public void setMemSamples(int memSamples) {
		this.memSamples = memSamples;
	}

	public KeyFilterArgs getKeyFilterArgs() {
		return keyFilterArgs;
	}

	public void setKeyFilterArgs(KeyFilterArgs keyFilterArgs) {
		this.keyFilterArgs = keyFilterArgs;
	}

}
