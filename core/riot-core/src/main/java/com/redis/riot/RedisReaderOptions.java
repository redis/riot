package com.redis.riot;

import java.util.Optional;

import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanReaderOptions;
import com.redis.spring.batch.reader.ScanSizeEstimatorOptions;

import picocli.CommandLine.Option;

public class RedisReaderOptions {

	@Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int queueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = ReaderOptions.DEFAULT_THREADS;

	@Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int batchSize = ReaderOptions.DEFAULT_CHUNK_SIZE;

	@Option(names = "--reader-skip-policy", description = "Policy to determine if some reading should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private StepSkipPolicy skipPolicy = StepSkipPolicy.NEVER;

	@Option(names = "--reader-skip-limit", description = "LIMIT skip policy: max number of failed items before considering reader has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit = ReaderOptions.DEFAULT_SKIP_LIMIT;

	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
	private String match = ScanReaderOptions.DEFAULT_MATCH;

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long count = ScanReaderOptions.DEFAULT_COUNT;

	@Option(names = "--scan-type", description = "SCAN TYPE option.", paramLabel = "<type>")
	private Optional<String> type = Optional.empty();

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

	public ScanReaderOptions readerOptions() {
		return ScanReaderOptions.builder().chunkSize(batchSize)
				.queueOptions(QueueOptions.builder().capacity(queueCapacity).build()).threads(threads)
				.skipPolicy(skipPolicy.getSkipPolicy()).skipLimit(skipLimit).match(match).count(count).type(type)
				.build();
	}

	public ScanSizeEstimatorOptions estimatorOptions() {
		return ScanSizeEstimatorOptions.builder().match(match).sampleSize(count).type(type).build();
	}

}
