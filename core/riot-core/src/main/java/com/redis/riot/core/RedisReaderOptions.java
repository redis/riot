package com.redis.riot.core;

import java.time.Duration;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.reader.KeyValueItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.step.FlushingChunkProvider;

import io.lettuce.core.ReadFrom;

public class RedisReaderOptions {

	public static final int DEFAULT_QUEUE_CAPACITY = RedisItemReader.DEFAULT_QUEUE_CAPACITY;

	public static final Duration DEFAULT_POLL_TIMEOUT = RedisItemReader.DEFAULT_POLL_TIMEOUT;

	public static final int DEFAULT_THREADS = RedisItemReader.DEFAULT_THREADS;

	public static final int DEFAULT_CHUNK_SIZE = RedisItemReader.DEFAULT_CHUNK_SIZE;

	public static final int DEFAULT_POOL_SIZE = KeyValueItemReader.DEFAULT_POOL_SIZE;

	public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = KeyValueItemReader.DEFAULT_MEMORY_USAGE_LIMIT;

	public static final int DEFAULT_MEMORY_USAGE_SAMPLES = KeyValueItemReader.DEFAULT_MEMORY_USAGE_SAMPLES;

	public static final OrderingStrategy DEFAULT_ORDERING = RedisItemReader.DEFAULT_ORDERING;

	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = RedisItemReader.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	public static final long DEFAULT_SCAN_COUNT = 1000;

	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

	private String keyPattern;

	private DataType keyType;

	private long scanCount = DEFAULT_SCAN_COUNT;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private int threads = DEFAULT_THREADS;

	private int chunkSize = DEFAULT_CHUNK_SIZE;

	private int poolSize = DEFAULT_POOL_SIZE;

	private ReadFrom readFrom;

	private DataSize memoryUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;

	private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

	private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

	private KeyFilterOptions keyFilterOptions = new KeyFilterOptions();

	public KeyFilterOptions getKeyFilterOptions() {
		return keyFilterOptions;
	}

	public void setKeyFilterOptions(KeyFilterOptions keyFilterOptions) {
		this.keyFilterOptions = keyFilterOptions;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration flushInterval) {
		this.flushInterval = flushInterval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public OrderingStrategy getOrderingStrategy() {
		return orderingStrategy;
	}

	public void setOrderingStrategy(OrderingStrategy orderingStrategy) {
		this.orderingStrategy = orderingStrategy;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String scanMatch) {
		this.keyPattern = scanMatch;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public DataType getKeyType() {
		return keyType;
	}

	public void setKeyType(DataType type) {
		this.keyType = type;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
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

}
