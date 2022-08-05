package com.redis.riot;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.ScanKeyItemReader;

import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Option;

public class RedisReaderOptions {

	private static final Logger log = Logger.getLogger(RedisReaderOptions.class.getName());

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	@Option(names = "--scan-type", description = "SCAN TYPE option", paramLabel = "<type>")
	private Optional<String> scanType = Optional.empty();
	@Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
	@Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = RedisItemReader.DEFAULT_THREADS;
	@Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int batchSize = RedisItemReader.DEFAULT_CHUNK_SIZE;
	@Option(names = "--sample-size", description = "Number of samples used to estimate dataset size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>", hidden = true)
	private int sampleSize = 100;
	@Option(names = "--reader-pool", description = "Max pool connections for reader process (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = 8;

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long scanCount) {
		this.scanCount = scanCount;
	}

	public String getScanMatch() {
		return scanMatch;
	}

	public void setScanMatch(String scanMatch) {
		this.scanMatch = scanMatch;
	}

	public Optional<String> getScanType() {
		return scanType;
	}

	public void setScanType(String scanType) {
		this.scanType = Optional.of(scanType);
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

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	public <K, V, T extends KeyValue<K, ?>> RedisItemReader.Builder<K, V, T> configureScanReader(
			RedisItemReader.Builder<K, V, T> builder) {
		log.log(Level.FINE, "Configuring scan reader with {0} {1} {2}",
				new Object[] { scanCount, scanMatch, scanType });
		builder.match(scanMatch);
		builder.count(scanCount);
		scanType.ifPresent(builder::type);
		return configureReader(builder);
	}

	public <K, V, B extends RedisItemReader.AbstractBuilder<K, V, ?, B>> B configureReader(B builder) {
		log.log(Level.FINE, "Configuring reader with threads: {0},  batch-size: {1}, queue-capacity: {2}",
				new Object[] { threads, batchSize, queueCapacity });
		builder.threads(threads);
		builder.chunkSize(batchSize);
		builder.valueQueueCapacity(queueCapacity);
		builder.poolConfig(poolConfig());
		return builder;
	}

	public <K, V> GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig() {
		GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolMaxTotal);
		log.log(Level.FINE, "Configuring reader with pool config {0}", config);
		return config;
	}

	@Override
	public String toString() {
		return "RedisReaderOptions [scanCount=" + scanCount + ", scanMatch=" + scanMatch + ", scanType=" + scanType
				+ ", queueCapacity=" + queueCapacity + ", threads=" + threads + ", batchSize=" + batchSize
				+ ", sampleSize=" + sampleSize + ", poolMaxTotal=" + poolMaxTotal + "]";
	}

}
