package com.redis.riot.cli.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.MemoryUsageOptions;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanOptions;

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

	@Option(names = "--mem-limit", description = "Maximum memory usage in MB for a key to be read (default: ${DEFAULT-VALUE}). Use 0 to disable memory usage checks.", paramLabel = "<MB>")
	private long memLimit = MemoryUsageOptions.DEFAULT_LIMIT.toMegabytes();

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int memSamples = MemoryUsageOptions.DEFAULT_SAMPLES;

	public int getMemSamples() {
		return memSamples;
	}

	public void setMemSamples(int memSamples) {
		this.memSamples = memSamples;
	}

	public long getMemLimit() {
		return memLimit;
	}

	public void setMemLimit(long memLimit) {
		this.memLimit = memLimit;
	}

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

	public PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

	public QueueOptions queueOptions() {
		return QueueOptions.builder().capacity(queueCapacity).build();
	}

	public ReaderOptions readerOptions() {
		ReaderOptions.Builder builder = ReaderOptions.builder();
		builder.chunkSize(chunkSize);
		builder.threads(threads);
		builder.poolOptions(poolOptions());
		builder.queueOptions(queueOptions());
		builder.readFrom(readFrom());
		builder.scanOptions(scanOptions());
		builder.memoryUsageOptions(memoryUsageOptions());
		return builder.build();
	}

	private MemoryUsageOptions memoryUsageOptions() {
		return MemoryUsageOptions.builder().limit(DataSize.ofMegabytes(memLimit)).samples(memSamples).build();
	}

	public Optional<ReadFrom> readFrom() {
		return readFrom.map(ReadFromEnum::getValue);
	}

	@Override
	public String toString() {
		return "RedisReaderOptions [queueCapacity=" + queueCapacity + ", threads=" + threads + ", chunkSize="
				+ chunkSize + ", scanMatch=" + scanMatch + ", scanCount=" + scanCount + ", scanType=" + scanType
				+ ", poolMaxTotal=" + poolMaxTotal + ", readFrom=" + readFrom + ", keyIncludes=" + keyIncludes
				+ ", keyExcludes=" + keyExcludes + ", keySlots=" + keySlots + ", memLimit=" + memLimit + ", memSamples="
				+ memSamples + "]";
	}

}
