package com.redis.riot.redis;

import java.time.Duration;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.LoggingWriteListener;
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
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;

public class Replication extends AbstractExport {

	public static final ReplicationType DEFAULT_TYPE = ReplicationType.DUMP;
	public static final ReplicationMode DEFAULT_MODE = ReplicationMode.SNAPSHOT;
	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;
	public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";
	public static final String STEP_LIVE = "live";
	public static final String STEP_SCAN = "scan";
	public static final String STEP_COMPARE = "compare";

	private static final String SOURCE_VAR = "source";
	private static final String TARGET_VAR = "target";

	private final Logger log = LoggerFactory.getLogger(Replication.class);
	private final Function<byte[], String> toString = BatchUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);

	private ReplicationMode mode = DEFAULT_MODE;
	private ReplicationType type = DEFAULT_TYPE;
	private boolean showDiffs;
	private CompareMode compareMode = DEFAULT_COMPARE_MODE;
	private Duration ttlTolerance = KeyComparatorOptions.DEFAULT_TTL_TOLERANCE;
	private RedisClientOptions targetRedisClientOptions = new RedisClientOptions();
	private ReadFrom targetReadFrom;
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	private RedisURI targetRedisURI;
	private AbstractRedisClient targetRedisClient;

	@Override
	protected boolean isStruct() {
		return type == ReplicationType.STRUCT;
	}

	@Override
	public void execute() throws Exception {
		try {
			targetRedisURI = targetRedisClientOptions.redisURI();
			targetRedisClient = targetRedisClientOptions.client(targetRedisURI);
			super.execute();
		} finally {
			targetRedisClient.close();
			targetRedisClient.getResources().shutdown();
		}
	}

	@Override
	protected StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext evaluationContext = super.evaluationContext();
		evaluationContext.setVariable(SOURCE_VAR, getRedisURI());
		evaluationContext.setVariable(TARGET_VAR, targetRedisURI);
		return evaluationContext;
	}

	@Override
	protected Job job() {
		ItemProcessor<KeyValue<byte[], Object>, KeyValue<byte[], Object>> processor = processor(
				ByteArrayCodec.INSTANCE);
		SimpleStepBuilder<KeyValue<byte[], Object>, KeyValue<byte[], Object>> scanStep = step(STEP_SCAN, reader(),
				writer()).processor(processor);
		RedisItemReader<byte[], byte[], KeyValue<byte[], Object>> liveReader = reader();
		liveReader.setMode(ReaderMode.LIVE);
		FlushingStepBuilder<KeyValue<byte[], Object>, KeyValue<byte[], Object>> liveStep = flushingStep(
				step(STEP_LIVE, liveReader, writer()).processor(processor));
		if (log.isInfoEnabled()) {
			addLoggingWriteListener(scanStep);
			addLoggingWriteListener(liveStep);
		}
		KeyComparisonStatusCountItemWriter compareWriter = new KeyComparisonStatusCountItemWriter();
		TaskletStep compareStep = step(STEP_COMPARE, comparisonReader(), compareWriter).build();
		switch (mode) {
		case COMPARE:
			return jobBuilder().start(compareStep).build();
		case LIVE:
			checkKeyspaceNotificationEnabled();
			SimpleFlow scanFlow = flow("scan").start(scanStep.build()).build();
			SimpleFlow liveFlow = flow("live").start(liveStep.build()).build();
			SimpleFlow replicateFlow = flow("replicate").split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow)
					.build();
			JobFlowBuilder live = jobBuilder().start(replicateFlow);
			if (shouldCompare()) {
				live.next(compareStep);
			}
			return live.build().build();
		case LIVEONLY:
			checkKeyspaceNotificationEnabled();
			return jobBuilder().start(liveStep.build()).build();
		case SNAPSHOT:
			SimpleJobBuilder snapshot = jobBuilder().start(scanStep.build());
			if (shouldCompare()) {
				snapshot.next(compareStep);
			}
			return snapshot.build();
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + mode);
		}
	}

	private void addLoggingWriteListener(SimpleStepBuilder<KeyValue<byte[], Object>, KeyValue<byte[], Object>> step) {
		step.listener(new LoggingWriteListener<>(this::log));
	}

	private void log(Chunk<? extends KeyValue<byte[], Object>> chunk) {
		chunk.getItems().stream().forEach(this::log);
	}

	private void log(KeyValue<byte[], Object> keyValue) {
		log.info("Wrote {}", toString.apply(keyValue.getKey()));
	}

	private FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name(name));
	}

	private boolean shouldCompare() {
		return compareMode != CompareMode.NONE && !isDryRun() && getProcessorOptions().isEmpty();
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
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
				.connection(getRedisClient())) {
			String config = connection.sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
					.getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
			if (!config.contains("K")) {
				log.error("Keyspace notifications not property configured ({}={}). Use the string KEA to enable them.",
						CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
			}
		} catch (RedisException e) {
			// CONFIG command might not be available. Ignore.
		}
	}

	private RedisItemReader<byte[], byte[], KeyValue<byte[], Object>> reader() {
		RedisItemReader<byte[], byte[], KeyValue<byte[], Object>> reader = createReader();
		reader.setClient(getRedisClient());
		configureReader(reader);
		return reader;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemReader<byte[], byte[], KeyValue<byte[], Object>> createReader() {
		if (isStruct()) {
			return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		}
		return (RedisItemReader) RedisItemReader.dump();
	}

	private KeyComparisonItemReader<byte[], byte[]> comparisonReader() {
		KeyComparisonItemReader<byte[], byte[]> reader = compareMode == CompareMode.FULL
				? RedisItemReader.compare(ByteArrayCodec.INSTANCE)
				: RedisItemReader.compareQuick(ByteArrayCodec.INSTANCE);
		configureReader(reader);
		reader.setClient(getRedisClient());
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

	private RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer() {
		RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer = createWriter();
		writer.setClient(targetRedisClient);
		writer(writer, writerOptions);
		return writer;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> createWriter() {
		if (isStruct()) {
			return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		}
		return (RedisItemWriter) RedisItemWriter.dump();
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

}
