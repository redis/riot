package com.redis.riot;

import java.time.Duration;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.unit.DataSize;

import com.redis.riot.core.PredicateItemProcessor;
import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;

import io.lettuce.core.codec.RedisCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisReaderArgs {

	public static final int DEFAULT_QUEUE_CAPACITY = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final Duration DEFAULT_POLL_TIMEOUT = AbstractPollableItemReader.DEFAULT_POLL_TIMEOUT;
	public static final int DEFAULT_THREADS = AbstractAsyncItemReader.DEFAULT_THREADS;
	public static final int DEFAULT_CHUNK_SIZE = AbstractAsyncItemReader.DEFAULT_CHUNK_SIZE;
	public static final int DEFAULT_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = KeyValueRead.DEFAULT_MEM_USAGE_LIMIT;
	public static final int DEFAULT_MEMORY_USAGE_SAMPLES = KeyValueRead.DEFAULT_MEM_USAGE_SAMPLES;
	public static final long DEFAULT_SCAN_COUNT = 1000;
	public static final Duration DEFAULT_FLUSH_INTERVAL = RedisItemReader.DEFAULT_FLUSH_INTERVAL;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = RedisItemReader.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	@Option(names = "--mode", description = "Source for keys: scan (key scan), live (scan + keyspace notifications), liveonly (keyspace notifications) (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReaderMode mode = RedisItemReader.DEFAULT_MODE;

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

	@Option(names = "--read-from", description = "Which Redis cluster nodes to read from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	private RedisReadFrom readFrom;

	@Option(names = "--mem-limit", description = "Max mem usage for a key to be read (default: ${DEFAULT-VALUE}). Use 0 for no limit, -1 to disable. Examples: 12KB, 5MB", paramLabel = "<size>")
	private DataSize memUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int memUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

	@Option(names = "--flush-interval", description = "Max duration in millis between flushes in live mode (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = DEFAULT_FLUSH_INTERVAL.toMillis();

	@Option(names = "--idle-timeout", description = "Min duration in seconds to consider reader complete in live mode (default: no timeout).", paramLabel = "<sec>")
	private long idleTimeout;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	@Option(names = "--read-retry", description = "Max number of times to try failed reads. 0 and 1 both mean no retry (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int retryLimit;

	@Option(names = "--read-skip", description = "Max number of failed reads before considering the reader has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit;

	@Option(names = "--read-pool", description = "Max pool connections used by Redis reader (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = DEFAULT_POOL_SIZE;

	@ArgGroup(exclusive = false)
	private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

	@Option(names = "--read-poll", description = "Interval in millis between queue polls (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private long pollTimeout = DEFAULT_POLL_TIMEOUT.toMillis();

	public <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		reader.setChunkSize(chunkSize);
		reader.setFlushInterval(Duration.ofMillis(flushInterval));
		if (idleTimeout > 0) {
			reader.setIdleTimeout(Duration.ofSeconds(idleTimeout));
		}
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setMode(mode);
		reader.setNotificationQueueCapacity(notificationQueueCapacity);
		reader.setPollTimeout(Duration.ofMillis(pollTimeout));
		reader.setPoolSize(poolSize);
		reader.setProcessor(keyProcessor(reader.getCodec()));
		reader.setQueueCapacity(queueCapacity);
		if (readFrom != null) {
			reader.setReadFrom(readFrom.getReadFrom());
		}
		reader.setRetryLimit(retryLimit);
		reader.setScanCount(scanCount);
		reader.setSkipLimit(skipLimit);
		reader.setThreads(threads);
		if (reader.getOperation() instanceof KeyValueRead) {
			@SuppressWarnings("rawtypes")
			KeyValueRead operation = (KeyValueRead) reader.getOperation();
			operation.setMemUsageLimit(memUsageLimit);
			operation.setMemUsageSamples(memUsageSamples);
		}
	}

	private <K> ItemProcessor<K, K> keyProcessor(RedisCodec<K, ?> codec) {
		return keyFilterArgs.predicate(codec).map(PredicateItemProcessor::new).orElse(null);
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

	public RedisReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(RedisReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public DataSize getMemUsageLimit() {
		return memUsageLimit;
	}

	public void setMemUsageLimit(DataSize memLimit) {
		this.memUsageLimit = memLimit;
	}

	public int getMemUsageSamples() {
		return memUsageSamples;
	}

	public void setMemUsageSamples(int memSamples) {
		this.memUsageSamples = memSamples;
	}

	public KeyFilterArgs getKeyFilterArgs() {
		return keyFilterArgs;
	}

	public void setKeyFilterArgs(KeyFilterArgs keyFilterArgs) {
		this.keyFilterArgs = keyFilterArgs;
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(long intervalMillis) {
		this.flushInterval = intervalMillis;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int size) {
		this.poolSize = size;
	}

	@Override
	public String toString() {
		return "RedisReaderArgs [mode=" + mode + ", keyPattern=" + keyPattern + ", keyType=" + keyType + ", scanCount="
				+ scanCount + ", queueCapacity=" + queueCapacity + ", threads=" + threads + ", chunkSize=" + chunkSize
				+ ", readFrom=" + readFrom + ", memUsageLimit=" + memUsageLimit + ", memUsageSamples=" + memUsageSamples
				+ ", flushInterval=" + flushInterval + ", idleTimeout=" + idleTimeout + ", notificationQueueCapacity="
				+ notificationQueueCapacity + ", retryLimit=" + retryLimit + ", skipLimit=" + skipLimit + ", poolSize="
				+ poolSize + ", keyFilterArgs=" + keyFilterArgs + ", pollTimeout=" + pollTimeout + "]";
	}

}
