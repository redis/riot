package com.redis.riot.redis;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RedisWriterOptions;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.AbstractKeyValueItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.KeyValueItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

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

	private ReplicationMode mode = DEFAULT_MODE;
	private ReplicationType type = DEFAULT_TYPE;
	private boolean showDiffs;
	private CompareMode compareMode = DEFAULT_COMPARE_MODE;
	private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;
	private RedisClientOptions targetRedisClientOptions = new RedisClientOptions();
	private ReadFrom targetReadFrom;
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	private RedisURI targetRedisURI;
	private AbstractRedisClient targetRedisClient;
	private StatefulRedisModulesConnection<String, String> targetRedisConnection;

	@Override
	protected boolean isStruct() {
		return type == ReplicationType.STRUCT;
	}

	@Override
	protected void open() {
		super.open();
		targetRedisURI = targetRedisClientOptions.redisURI();
		targetRedisClient = targetRedisClientOptions.client(targetRedisURI);
		targetRedisConnection = RedisModulesUtils.connection(targetRedisClient);
	}

	@Override
	protected StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext evaluationContext = super.evaluationContext();
		evaluationContext.setVariable(SOURCE_VAR, getRedisURI());
		evaluationContext.setVariable(TARGET_VAR, targetRedisURI);
		return evaluationContext;
	}

	@Override
	protected void close() {
		try {
			targetRedisConnection.close();
		} finally {
			targetRedisClient.close();
			targetRedisClient.getResources().shutdown();
		}
		super.close();
	}

	@Override
	protected Job job() {
		FaultTolerantStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep = step(STEP_SCAN, reader("scan-reader"));
		RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = reader("live-reader");
		reader.setMode(RedisItemReader.Mode.LIVE);
		FaultTolerantStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep = step(STEP_LIVE, reader);
		KeyComparisonStatusCountItemWriter comparisonWriter = new KeyComparisonStatusCountItemWriter();
		FaultTolerantStepBuilder<KeyComparison, KeyComparison> compareStep = step(STEP_COMPARE, comparisonReader(),
				null, comparisonWriter);
		if (showDiffs) {
			compareStep.listener(new KeyComparisonDiffLogger());
		}
		compareStep.listener(new KeyComparisonSummaryLogger(comparisonWriter));
		switch (mode) {
		case COMPARE:
			return jobBuilder().start(compareStep.build()).build();
		case LIVE:
			checkKeyspaceNotificationEnabled();
			SimpleFlow scanFlow = flow("scan").start(scanStep.build()).build();
			SimpleFlow liveFlow = flow("live").start(liveStep.build()).build();
			SimpleFlow replicateFlow = flow("replicate").split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow)
					.build();
			JobFlowBuilder live = jobBuilder().start(replicateFlow);
			if (shouldCompare()) {
				live.next(compareStep.build());
			}
			return live.build().build();
		case LIVEONLY:
			checkKeyspaceNotificationEnabled();
			return jobBuilder().start(liveStep.build()).build();
		case SNAPSHOT:
			SimpleJobBuilder snapshot = jobBuilder().start(scanStep.build());
			if (shouldCompare()) {
				snapshot.next(compareStep.build());
			}
			return snapshot.build();
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + mode);
		}
	}

	private FlowBuilder<SimpleFlow> flow(String name) {
		return new FlowBuilder<>(name(name));
	}

	private boolean shouldCompare() {
		return compareMode != CompareMode.NONE && !isDryRun();
	}

	private FaultTolerantStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(String name,
			RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader) {
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = writer();
		ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> processor = new FunctionItemProcessor<>(
				processor(ByteArrayCodec.INSTANCE));
		FaultTolerantStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(name, reader, processor, writer);
		if (log.isDebugEnabled()) {
			step.listener(new KeyValueWriteListener<>(reader.getCodec(), log));
		}
		return step;
	}

	private void checkKeyspaceNotificationEnabled() {
		try {
			String config = getRedisConnection().sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
					.getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
			if (!config.contains("K")) {
				log.error(
						"Keyspace notifications not property configured ({}={}). Make sure it contains at least \"K\".",
						CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
			}
		} catch (RedisException e) {
			// CONFIG command might not be available. Ignore.
		}
	}

	private RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader(String name) {
		AbstractKeyValueItemReader<byte[], byte[]> reader = reader(getRedisClient());
		configureReader(name(name), reader);
		return reader;
	}

	private AbstractKeyValueItemReader<byte[], byte[]> reader(AbstractRedisClient client) {
		if (isStruct()) {
			return new StructItemReader<>(client, ByteArrayCodec.INSTANCE);
		}
		return new DumpItemReader(client);
	}

	private KeyComparisonItemReader comparisonReader() {
		AbstractKeyValueItemReader<String, String> sourceReader = comparisonKeyValueReader(getRedisClient());
		configureReader("source-comparison-reader", sourceReader);
		AbstractKeyValueItemReader<String, String> targetReader = comparisonKeyValueReader(targetRedisClient);
		targetReader.setReadFrom(targetReadFrom);
		targetReader.setPoolSize(writerOptions.getPoolSize());
		KeyComparisonItemReader comparisonReader = new KeyComparisonItemReader(sourceReader, targetReader);
		configureReader("comparison-reader", comparisonReader);
		comparisonReader.setProcessor(processor(StringCodec.UTF8));
		comparisonReader.setTtlTolerance(ttlTolerance);
		comparisonReader.setCompareStreamMessageIds(!getProcessorOptions().isDropStreamMessageId());
		return comparisonReader;
	}

	private AbstractKeyValueItemReader<String, String> comparisonKeyValueReader(AbstractRedisClient client) {
		if (compareMode == CompareMode.FULL) {
			return new StructItemReader<>(client, StringCodec.UTF8);
		}
		return new KeyTypeItemReader<>(client, StringCodec.UTF8);
	}

	private RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer() {
		KeyValueItemWriter<byte[], byte[]> writer = writer(targetRedisClient);
		return writer(writer, writerOptions);
	}

	private KeyValueItemWriter<byte[], byte[]> writer(AbstractRedisClient client) {
		if (isStruct()) {
			return new StructItemWriter<>(client, ByteArrayCodec.INSTANCE);
		}
		return new DumpItemWriter(client);
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
