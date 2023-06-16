package com.redis.riot.cli.common;

import java.util.Optional;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.Option;

public class RedisReaderOptions {

	@Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = RedisItemReader.DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int chunkSize = RedisItemReader.DEFAULT_CHUNK_SIZE;

	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = ScanKeyItemReader.DEFAULT_MATCH;

	@Option(names = "--scan-samples", description = "Number of samples to estimate scan size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanSampleSize = ScanSizeEstimator.DEFAULT_SAMPLE_SIZE;

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = ScanKeyItemReader.DEFAULT_COUNT;

	@Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
	private Optional<String> scanType = Optional.empty();

	@Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	public long getScanSampleSize() {
		return scanSampleSize;
	}

	public void setScanSampleSize(long scanSampleSize) {
		this.scanSampleSize = scanSampleSize;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public String getScanMatch() {
		return scanMatch;
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

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setScanMatch(String match) {
		this.scanMatch = match;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public Optional<String> getScanType() {
		return scanType;
	}

	public void setScanType(Optional<String> type) {
		this.scanType = type;
	}

}
