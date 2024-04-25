package com.redis.riot.core;

import java.time.Duration;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.operation.KeyValueRead;
import com.redis.spring.batch.reader.AbstractPollableItemReader;

import io.lettuce.core.ReadFrom;

public class RedisReaderOptions {

	public static final int DEFAULT_QUEUE_CAPACITY = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final Duration DEFAULT_POLL_TIMEOUT = AbstractPollableItemReader.DEFAULT_POLL_TIMEOUT;
	public static final int DEFAULT_THREADS = RedisItemReader.DEFAULT_THREADS;
	public static final int DEFAULT_CHUNK_SIZE = RedisItemReader.DEFAULT_CHUNK_SIZE;
	public static final int DEFAULT_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = KeyValueRead.DEFAULT_MEM_USAGE_LIMIT;
	public static final int DEFAULT_MEMORY_USAGE_SAMPLES = KeyValueRead.DEFAULT_MEM_USAGE_SAMPLES;
	public static final long DEFAULT_SCAN_COUNT = 1000;

	private String keyPattern;
	private String keyType;
	private long scanCount = DEFAULT_SCAN_COUNT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int poolSize = DEFAULT_POOL_SIZE;
	private ReadFrom readFrom;
	private DataSize memoryUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;
	private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;
	private KeyFilterOptions keyFilterOptions = new KeyFilterOptions();

	public <K> void configure(RedisItemReader<K, ?, ?> reader) {
		reader.setChunkSize(chunkSize);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setPollTimeout(pollTimeout);
		reader.setQueueCapacity(queueCapacity);
		reader.setReadFrom(readFrom);
		reader.setScanCount(scanCount);
		reader.setThreads(threads);
		if (reader.getOperation() instanceof KeyValueRead) {
			KeyValueRead<?, ?, ?> operation = (KeyValueRead<?, ?, ?>) reader.getOperation();
			operation.setMemUsageLimit(memoryUsageLimit);
			operation.setMemUsageSamples(memoryUsageSamples);
		}
		reader.setPoolSize(poolSize);
		reader.setKeyProcessor(keyFilterOptions.processor(reader.getCodec()));
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String pattern) {
		this.keyPattern = pattern;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int capacity) {
		this.queueCapacity = capacity;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
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

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public DataSize getMemoryUsageLimit() {
		return memoryUsageLimit;
	}

	public void setMemoryUsageLimit(DataSize memoryUsageLimit) {
		this.memoryUsageLimit = memoryUsageLimit;
	}

	public int getMemoryUsageSamples() {
		return memoryUsageSamples;
	}

	public void setMemoryUsageSamples(int memoryUsageSamples) {
		this.memoryUsageSamples = memoryUsageSamples;
	}

	public KeyFilterOptions getKeyFilterOptions() {
		return keyFilterOptions;
	}

	public void setKeyFilterOptions(KeyFilterOptions options) {
		this.keyFilterOptions = options;
	}

}
