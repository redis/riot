package com.redis.riot;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.builder.RedisItemReaderBuilder;
import com.redis.spring.batch.builder.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.ScanKeyItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator.ScanSizeEstimatorBuilder;

import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Option;

public class RedisReaderOptions {

	public enum ScanType {
		STRING(DataStructure.STRING), LIST(DataStructure.LIST), SET(DataStructure.SET), ZSET(DataStructure.ZSET),
		HASH(DataStructure.HASH), STREAM(DataStructure.STREAM);

		private String name;

		private ScanType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	private static final Logger log = LoggerFactory.getLogger(RedisReaderOptions.class);

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	@Option(names = "--scan-type", description = "SCAN TYPE option: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ScanType scanType;
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

	public void setScanType(ScanType scanType) {
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

	@SuppressWarnings("rawtypes")
	public <B extends ScanRedisItemReaderBuilder> B configureScanReader(B builder) {
		log.debug("Configuring scan reader with {} {} {}", scanCount, scanMatch, scanType);
		builder.match(scanMatch);
		builder.count(scanCount);
		if (scanType != null) {
			builder.type(scanType.getName());
		}
		return configureReader(builder);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <B extends RedisItemReaderBuilder> B configureReader(B builder) {
		log.debug("Configuring reader with threads: {},  batch-size: {}, queue-capacity: {}", threads, batchSize,
				queueCapacity);
		builder.threads(threads);
		builder.chunkSize(batchSize);
		builder.valueQueueCapacity(queueCapacity);
		builder.poolConfig(poolConfig());
		return builder;
	}

	public GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig() {
		GenericObjectPoolConfig<StatefulConnection<String, String>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolMaxTotal);
		log.debug("Configuring reader with pool config {}", config);
		return config;
	}

	public Supplier<Long> initialMaxSupplier(ScanSizeEstimatorBuilder estimator) {
		return () -> {
			try {
				estimator.match(scanMatch).sampleSize(sampleSize);
				if (scanType != null) {
					estimator.type(scanType.getName());
				}
				return estimator.build().call();
			} catch (Exception e) {
				log.warn("Could not estimate scan size", e);
				return null;
			}
		};
	}

}
