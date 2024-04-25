package com.redis.riot.redis;

import java.time.Duration;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RedisWriterOptions;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.reader.KeyComparatorOptions;
import com.redis.spring.batch.reader.KeyComparatorOptions.StreamMessageIdPolicy;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.ByteArrayCodec;

public class Replication extends AbstractExport {

	public static final Duration DEFAULT_FLUSH_INTERVAL = RedisItemReader.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = RedisItemReader.DEFAULT_IDLE_TIMEOUT;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = RedisItemReader.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	public static final ReplicationType DEFAULT_TYPE = ReplicationType.DUMP;
	public static final ReplicationMode DEFAULT_MODE = ReplicationMode.SNAPSHOT;
	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;
	public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";
	public static final String FLOW_LIVE = "live-flow";
	public static final String FLOW_SCAN = "scan-flow";
	public static final String FLOW_REPLICATE = "replicate-flow";
	public static final String STEP_LIVE = "live-step";
	public static final String STEP_SCAN = "scan-step";
	public static final String STEP_COMPARE = "compare-step";

	private static final String SOURCE_VAR = "source";
	private static final String TARGET_VAR = "target";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private ReplicationMode mode = DEFAULT_MODE;
	private ReplicationType type = DEFAULT_TYPE;
	private boolean showDiffs;
	private CompareMode compareMode = DEFAULT_COMPARE_MODE;
	private Duration ttlTolerance = KeyComparatorOptions.DEFAULT_TTL_TOLERANCE;
	private RedisClientOptions targetRedisClientOptions = new RedisClientOptions();
	private ReadFrom targetReadFrom;
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	private AbstractRedisClient targetRedisClient;

	@Override
	public void afterPropertiesSet() throws Exception {
		targetRedisClient = targetRedisClientOptions.redisClient();
		super.afterPropertiesSet();
	}

	@Override
	protected StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext context = super.evaluationContext();
		context.setVariable(SOURCE_VAR, getRedisClientOptions().getRedisURI());
		context.setVariable(TARGET_VAR, targetRedisClientOptions.getRedisURI());
		return context;
	}

	@Override
	public void close() {
		if (targetRedisClient != null) {
			targetRedisClient.close();
			targetRedisClient.getResources().shutdown();
		}
		super.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Job job() {
		ItemProcessor processor = processor(ByteArrayCodec.INSTANCE);
		RedisItemReader reader = reader();
		configure(reader);
		RedisItemWriter writer = writer();
		configure(writer);
		SimpleStepBuilder scanStep = step(STEP_SCAN, reader, writer).processor(processor);
		RedisItemReader liveReader = reader();
		configure(liveReader);
		liveReader.setMode(ReaderMode.LIVE);
		RedisItemWriter liveWriter = writer();
		configure(liveWriter);
		FlushingStepBuilder liveStep = new FlushingStepBuilder<>(step(STEP_LIVE, liveReader, liveWriter))
				.processor(processor);
		liveStep.flushInterval(flushInterval);
		liveStep.idleTimeout(idleTimeout);
		LoggingWriteListener listener = new LoggingWriteListener();
		scanStep.listener(listener);
		liveStep.listener(listener);
		KeyComparisonStatusCountItemWriter compareWriter = new KeyComparisonStatusCountItemWriter();
		TaskletStep compareStep = step(STEP_COMPARE, comparisonReader(), compareWriter).build();
		switch (mode) {
		case COMPARE:
			return jobBuilder().start(compareStep).build();
		case LIVE:
			checkKeyspaceNotificationEnabled();
			SimpleFlow scanFlow = flow(FLOW_SCAN).start(scanStep.build()).build();
			SimpleFlow liveFlow = flow(FLOW_LIVE).start(liveStep.build()).build();
			SimpleFlow replicateFlow = flow(FLOW_REPLICATE).split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow)
					.build();
			JobFlowBuilder live = jobBuilder().start(replicateFlow);
			if (shouldCompare() && processor == null) {
				live.next(compareStep);
			}
			return live.build().build();
		case LIVEONLY:
			checkKeyspaceNotificationEnabled();
			return jobBuilder().start(liveStep.build()).build();
		case SNAPSHOT:
			SimpleJobBuilder snapshot = jobBuilder().start(scanStep.build());
			if (shouldCompare() && processor == null) {
				snapshot.next(compareStep);
			}
			return snapshot.build();
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + mode);
		}
	}

	public static class LoggingWriteListener implements ItemWriteListener<KeyValue<byte[], Object>> {

		private final Logger log = LoggerFactory.getLogger(getClass());
		private final Function<byte[], String> toString = BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);

		@Override
		public void afterWrite(Chunk<? extends KeyValue<byte[], Object>> chunk) {
			chunk.forEach(t -> log.info("Wrote {}", toString.apply(t.getKey())));
		}

	}

	private FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name(name));
	}

	private boolean shouldCompare() {
		return compareMode != CompareMode.NONE && !isDryRun();
	}

	@Override
	protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
		FaultTolerantStepBuilder<I, O> step = super.step(name, reader, writer);
		if (STEP_COMPARE.equals(name)) {
			if (showDiffs) {
				step.listener(new KeyComparisonDiffLogger<>(ByteArrayCodec.INSTANCE));
			}
			step.listener(new KeyComparisonSummaryLogger((KeyComparisonStatusCountItemWriter) writer));
		}
		return step;
	}

	private void checkKeyspaceNotificationEnabled() {
		try {
			String config = redisCommands.configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
					.getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
			if (!config.contains("K") || !config.contains("E")) {
				log.error("Keyspace notifications not property configured ({}={}). Use the string KEA to enable them.",
						CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
			}
		} catch (RedisException e) {
			// CONFIG command might not be available. Ignore.
		}
	}

	@Override
	protected <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		super.configure(reader);
		reader.setFlushInterval(flushInterval);
		reader.setIdleTimeout(idleTimeout);
		reader.setNotificationQueueCapacity(notificationQueueCapacity);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemReader<byte[], byte[], KeyValue<byte[], Object>> reader() {
		if (type == ReplicationType.STRUCT) {
			return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		}
		return (RedisItemReader) RedisItemReader.dump();
	}

	private KeyComparisonItemReader<byte[], byte[]> comparisonReader() {
		KeyComparisonItemReader<byte[], byte[]> reader = compareMode == CompareMode.FULL
				? RedisItemReader.compare(ByteArrayCodec.INSTANCE)
				: RedisItemReader.compareQuick(ByteArrayCodec.INSTANCE);
		configure(reader);
		reader.setTargetClient(targetRedisClient);
		reader.setTargetPoolSize(writerOptions.getPoolSize());
		reader.setTargetReadFrom(targetReadFrom);
		reader.setComparatorOptions(comparatorOptions());
		return reader;
	}

	private KeyComparatorOptions comparatorOptions() {
		KeyComparatorOptions options = new KeyComparatorOptions();
		options.setTtlTolerance(ttlTolerance);
		options.setStreamMessageIdPolicy(streamMessageIdPolicy());
		return options;
	}

	private StreamMessageIdPolicy streamMessageIdPolicy() {
		if (getProcessorOptions().isDropStreamMessageId()) {
			return StreamMessageIdPolicy.IGNORE;
		}
		return StreamMessageIdPolicy.COMPARE;
	}

	@Override
	protected <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		super.configure(writer);
		writer.setClient(targetRedisClient);
		writerOptions.configure(writer);
	}

	private RedisItemWriter<byte[], byte[], ? extends KeyValue<byte[], ?>> writer() {
		if (type == ReplicationType.STRUCT) {
			return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		}
		return RedisItemWriter.dump();
	}

	public CompareMode getCompareMode() {
		return compareMode;
	}

	public void setCompareMode(CompareMode mode) {
		this.compareMode = mode;
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiff) {
		this.showDiffs = showDiff;
	}

	public Duration getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(Duration ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public RedisClientOptions getTargetRedisClientOptions() {
		return targetRedisClientOptions;
	}

	public void setTargetRedisClientOptions(RedisClientOptions targetRedisOptions) {
		this.targetRedisClientOptions = targetRedisOptions;
	}

	public ReadFrom getTargetReadFrom() {
		return targetReadFrom;
	}

	public void setTargetReadFrom(ReadFrom targetReadFrom) {
		this.targetReadFrom = targetReadFrom;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	public ReplicationMode getMode() {
		return mode;
	}

	public ReplicationType getType() {
		return type;
	}

	public void setMode(ReplicationMode mode) {
		this.mode = mode;
	}

	public void setType(ReplicationType type) {
		this.type = type;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

}
