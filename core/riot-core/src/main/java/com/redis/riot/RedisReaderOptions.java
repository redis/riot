package com.redis.riot;

import java.util.Optional;

import org.springframework.batch.core.step.skip.SkipPolicy;

import com.redis.spring.batch.common.FaultToleranceOptions;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ReaderOptions.Builder;
import com.redis.spring.batch.reader.ScanOptions;
import com.redis.spring.batch.reader.ScanSizeEstimatorOptions;

import picocli.CommandLine.Option;

public class RedisReaderOptions {

	@Option(names = "--read-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--read-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = ReaderOptions.DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int batchSize = ReaderOptions.DEFAULT_CHUNK_SIZE;

	@Option(names = "--read-skip-policy", description = "Policy to determine if some reading should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private StepSkipPolicy skipPolicy = StepSkipPolicy.NEVER;

	@Option(names = "--read-skip-limit", description = "LIMIT skip policy: max number of failed items before considering reader has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<l>")
	private int skipLimit = FaultToleranceOptions.DEFAULT_SKIP_LIMIT;

	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String match = ScanOptions.DEFAULT_MATCH;

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long count = ScanOptions.DEFAULT_COUNT;

	@Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
	private Optional<String> type = Optional.empty();

	@Option(names = "--read-pool", description = "Max connections for reader pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public Optional<String> getType() {
		return type;
	}

	public void setType(Optional<String> type) {
		this.type = type;
	}

	public ReaderOptions readerOptions() {
		Builder builder = ReaderOptions.builder();
		builder.chunkSize(batchSize);
		builder.threads(threads);
		builder.faultToleranceOptions(faultToleranceOptions());
		builder.queueOptions(queueOptions());
		builder.poolOptions(poolOptions());
		return builder.build();
	}

	private QueueOptions queueOptions() {
		return QueueOptions.builder().capacity(queueCapacity).build();
	}

	private PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

	private FaultToleranceOptions faultToleranceOptions() {
		return FaultToleranceOptions.builder().skipPolicy(skipPolicy()).skipLimit(skipLimit).build();
	}

	private SkipPolicy skipPolicy() {
		return TransferOptions.skipPolicy(skipPolicy, skipLimit);
	}

	public ScanOptions scanOptions() {
		return ScanOptions.builder().match(match).count(count).type(type).build();
	}

	public ScanSizeEstimatorOptions scanSizeEstimatorOptions() {
		ScanSizeEstimatorOptions.Builder builder = ScanSizeEstimatorOptions.builder();
		builder.match(match);
		builder.sampleSize(count);
		builder.type(type);
		return builder.build();
	}

}
