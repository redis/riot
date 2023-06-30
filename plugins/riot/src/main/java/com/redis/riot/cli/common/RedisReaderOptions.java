package com.redis.riot.cli.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import io.lettuce.core.ReadFrom;
import picocli.CommandLine.Option;

public class RedisReaderOptions {

	@Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = ReaderOptions.DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int chunkSize = ReaderOptions.DEFAULT_CHUNK_SIZE;

	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = ScanOptions.DEFAULT_MATCH;

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = ScanOptions.DEFAULT_COUNT;

	@Option(names = "--scan-samples", description = "Number of samples to estimate scan size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanSampleSize = ScanSizeEstimator.DEFAULT_SAMPLE_SIZE;

	@Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
	private Optional<String> scanType = Optional.empty();

	@Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	@Option(names = "--read-from", description = "Which Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	private Optional<ReadFromEnum> readFrom = Optional.empty();

	@Option(names = "--key-include", arity = "1..*", description = "Regexes to match keys for inclusion.", paramLabel = "<exp>")
	private List<String> keyIncludes = new ArrayList<>();

	@Option(names = "--key-exclude", arity = "1..*", description = "Regexes to match keys for exclusion.", paramLabel = "<exp>")
	private List<String> keyExcludes = new ArrayList<>();

	@Option(names = "--key-slots", arity = "1..*", description = "Key slot ranges to filter keyspace notifications.", paramLabel = "<range>")
	private List<IntRange> keySlots = new ArrayList<>();

	public List<IntRange> getKeySlots() {
		return keySlots;
	}

	public void setKeySlots(List<IntRange> keySlots) {
		this.keySlots = keySlots;
	}

	public List<String> getKeyIncludes() {
		return keyIncludes;
	}

	public void setKeyIncludes(List<String> keyIncludes) {
		this.keyIncludes = keyIncludes;
	}

	public List<String> getKeyExcludes() {
		return keyExcludes;
	}

	public void setKeyExcludes(List<String> keyExcludes) {
		this.keyExcludes = keyExcludes;
	}

	public Optional<ReadFromEnum> getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(Optional<ReadFromEnum> readFrom) {
		this.readFrom = readFrom;
	}

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

	public ScanOptions scanOptions() {
		return ScanOptions.builder().count(scanCount).match(scanMatch).count(scanCount).build();
	}

	public ScanOptions estimatorOptions() {
		ScanOptions scanOptions = scanOptions();
		scanOptions.setCount(scanSampleSize);
		return scanOptions;
	}

	public PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

	public QueueOptions queueOptions() {
		return QueueOptions.builder().capacity(queueCapacity).build();
	}

	public ReaderOptions readerOptions() {
		return ReaderOptions.builder().chunkSize(chunkSize).threads(threads).poolOptions(poolOptions())
				.queueOptions(queueOptions()).readFrom(readFrom()).build();
	}

	public Optional<ReadFrom> readFrom() {
		return readFrom.map(ReadFromEnum::getValue);
	}

}
