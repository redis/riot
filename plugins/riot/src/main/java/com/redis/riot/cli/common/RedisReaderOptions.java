package com.redis.riot.cli.common;

import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimatorOptions;

import picocli.CommandLine.Option;

public class RedisReaderOptions {

	public static final int DEFAULT_QUEUE_CAPACITY = QueueOptions.DEFAULT_CAPACITY;
	public static final int DEFAULT_THREADS = StepOptions.DEFAULT_THREADS;
	public static final int DEFAULT_CHUNK_SIZE = StepOptions.DEFAULT_CHUNK_SIZE;
	public static final StepSkipPolicy DEFAULT_SKIP_POLICY = StepSkipPolicy.NEVER;
	public static final int DEFAULT_SKIP_LIMIT = StepOptions.DEFAULT_SKIP_LIMIT;
	public static final String DEFAULT_SCAN_MATCH = ScanOptions.DEFAULT_MATCH;
	public static final long DEFAULT_SCAN_COUNT = ScanOptions.DEFAULT_COUNT;
	public static final int DEFAULT_POOL_MAX_TOTAL = PoolOptions.DEFAULT_MAX_TOTAL;

	@Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	@Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int chunkSize = DEFAULT_CHUNK_SIZE;

	@Option(names = "--read-skip-policy", description = "Policy to determine if some reading should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private StepSkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

	@Option(names = "--read-skip-limit", description = "LIMIT skip policy: max number of failed items before considering reader has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<l>")
	private int skipLimit = DEFAULT_SKIP_LIMIT;

	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String scanMatch = DEFAULT_SCAN_MATCH;

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = DEFAULT_SCAN_COUNT;

	@Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
	private Optional<String> type = Optional.empty();

	@Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = DEFAULT_POOL_MAX_TOTAL;

	public String getScanMatch() {
		return scanMatch;
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

	public Optional<String> getType() {
		return type;
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	private StepOptions stepOptions() {
		return StepOptions.builder().chunkSize(chunkSize).threads(threads).skipPolicy(skipPolicy()).skipLimit(skipLimit)
				.build();
	}

	private QueueOptions queueOptions() {
		return QueueOptions.builder().capacity(queueCapacity).build();
	}

	public PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

	private SkipPolicy skipPolicy() {
		return TransferOptions.skipPolicy(skipPolicy, skipLimit);
	}

	public ScanOptions scanOptions() {
		return ScanOptions.builder().match(scanMatch).count(scanCount).type(type).build();
	}

	public ScanSizeEstimatorOptions scanSizeEstimatorOptions() {
		return ScanSizeEstimatorOptions.builder().match(scanMatch).sampleSize(scanCount).type(type).build();
	}

	public ReaderOptions readerOptions() {
		return ReaderOptions.builder().poolOptions(poolOptions()).queueOptions(queueOptions())
				.stepOptions(stepOptions()).build();
	}

}
