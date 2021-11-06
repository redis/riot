package com.redis.riot;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.builder.RedisItemReaderBuilder;
import com.redis.spring.batch.builder.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.ScanKeyItemReader;
import com.redis.spring.batch.support.ScanSizeEstimator.ScanSizeEstimatorBuilder;

import io.lettuce.core.api.StatefulConnection;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
@Data
public class RedisReaderOptions {

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	@Option(names = "--scan-type", description = "SCAN TYPE option: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private Type scanType;
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

	@SuppressWarnings("rawtypes")
	public <B extends ScanRedisItemReaderBuilder> B configureScanReader(B builder) {
		log.info("Configuring scan reader with {} {} {}", scanCount, scanMatch, scanType);
		builder.scanMatch(scanMatch);
		builder.scanCount(scanCount);
		if (scanType != null) {
			builder.scanType(scanType.name().toLowerCase());
		}
		return configureReader(builder);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <B extends RedisItemReaderBuilder> B configureReader(B builder) {
		log.info("Configuring reader with threads: {},  batch-size: {}, queue-capacity: {}", threads, batchSize,
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
		log.info("Configuring reader with pool config {}", config);
		return config;
	}

	public ScanSizeEstimatorBuilder configureEstimator(ScanSizeEstimatorBuilder builder) {
		builder.match(scanMatch).sampleSize(sampleSize);
		if (scanType != null) {
			builder.type(scanType.name().toLowerCase());
		}
		return builder;
	}

	public Supplier<Long> initialMaxSupplier(ScanSizeEstimatorBuilder estimator) {
		return () -> {
			try {
				return configureEstimator(estimator).build().call();
			} catch (Exception e) {
				log.warn("Could not estimate scan size", e);
				return null;
			}
		};
	}

}
