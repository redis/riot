package com.redis.riot;

import java.time.temporal.ChronoUnit;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.RiotDuration;
import com.redis.riot.core.processor.FunctionPredicate;
import com.redis.riot.core.processor.PredicateOperator;
import com.redis.spring.batch.item.AbstractAsyncItemStreamSupport;
import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.KeyEvent;

import io.lettuce.core.codec.RedisCodec;
import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@ToString
public class RedisReaderArgs {

	public static final long DEFAULT_SCAN_COUNT = 1000;
	public static final int DEFAULT_QUEUE_CAPACITY = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final int DEFAULT_THREADS = AbstractAsyncItemStreamSupport.DEFAULT_THREADS;
	public static final int DEFAULT_CHUNK_SIZE = AbstractAsyncItemStreamSupport.DEFAULT_CHUNK_SIZE;
	public static final RiotDuration DEFAULT_POLL_TIMEOUT = RiotDuration
			.of(AbstractPollableItemReader.DEFAULT_POLL_TIMEOUT, ChronoUnit.MILLIS);

	@Option(names = "--key-pattern", description = "Pattern of keys to read (default: *).", paramLabel = "<glob>")
	private String keyPattern;

	@Option(names = "--key-type", description = "Type of keys to read (default: all types).", paramLabel = "<type>")
	private String keyType;

	@Option(names = "--scan-count", description = "How many keys to read at once on each SCAN call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private long scanCount = DEFAULT_SCAN_COUNT;

	@Option(names = "--read-queue", description = "Max items that reader threads can queue up (default: ${DEFAULT-VALUE}). When the queue is full the threads wait for space to become available.", paramLabel = "<int>")
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	@Option(names = "--read-threads", description = "How many value reader threads to use in parallel (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = DEFAULT_THREADS;

	@Option(names = "--read-batch", description = "Number of values each reader thread should read in a pipelined call (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int chunkSize = DEFAULT_CHUNK_SIZE;

	@Option(names = "--read-retry", description = "Max number of times to try failed reads. 0 and 1 both mean no retry (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int retryLimit;

	@Option(names = "--read-skip", description = "Max number of failed reads before considering the reader has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit;

	@Option(names = "--read-poll", description = "Interval between queue polls (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>", hidden = true)
	private RiotDuration pollTimeout = DEFAULT_POLL_TIMEOUT;

	@ArgGroup(exclusive = false)
	private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

	public <K> void configure(RedisItemReader<K, ?> reader) {
		reader.setChunkSize(chunkSize);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setQueueCapacity(queueCapacity);
		reader.setRetryLimit(retryLimit);
		reader.setScanCount(scanCount);
		reader.setSkipLimit(skipLimit);
		reader.setThreads(threads);
		reader.setPollTimeout(pollTimeout.getValue());
		reader.setProcessor(keyProcessor(reader.getCodec(), keyFilterArgs));
	}

	private <K> ItemProcessor<KeyEvent<K>, KeyEvent<K>> keyProcessor(RedisCodec<K, ?> codec, KeyFilterArgs args) {
		return args.predicate(codec).map(p -> new FunctionPredicate<KeyEvent<K>, K>(KeyEvent::getKey, p))
				.map(PredicateOperator::new).map(FunctionItemProcessor::new).orElse(null);
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

	public void setScanCount(long scanCount) {
		this.scanCount = scanCount;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String scanType) {
		this.keyType = scanType;
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

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public RiotDuration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(RiotDuration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public KeyFilterArgs getKeyFilterArgs() {
		return keyFilterArgs;
	}

	public void setKeyFilterArgs(KeyFilterArgs keyFilterArgs) {
		this.keyFilterArgs = keyFilterArgs;
	}

}
