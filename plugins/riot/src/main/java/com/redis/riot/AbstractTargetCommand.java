package com.redis.riot;

import java.time.Duration;
import java.util.Collection;

import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.CompareStatusItemWriter.StatusCount;
import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractTargetCommand extends AbstractRedisCommand {

	public static final Duration DEFAULT_TTL_TOLERANCE = DefaultKeyComparator.DEFAULT_TTL_TOLERANCE;
	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	public static final String COMPARE_STEP_NAME = "compare";
	public static final boolean DEFAULT_COMPARE_STREAM_MESSAGE_ID = true;

	private static final String SOURCE_VAR = "source";
	private static final String TARGET_VAR = "target";
	private static final String COMPARE_TASK_NAME = "Comparing";
	private static final String STATUS_DELIMITER = " | ";

	@Parameters(arity = "1", index = "0", description = "Source server URI.", paramLabel = "SOURCE")
	private RedisURI sourceRedisURI;

	@ArgGroup(exclusive = false)
	private SourceRedisArgs sourceRedisArgs = new SourceRedisArgs();

	@Parameters(arity = "1", index = "1", description = "Target server URI.", paramLabel = "TARGET")
	private RedisURI targetRedisURI;

	@ArgGroup(exclusive = false)
	private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

	@ArgGroup(exclusive = false)
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
	private boolean showDiffs;

	@Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to consider keys equal (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long ttlToleranceMillis = DEFAULT_TTL_TOLERANCE.toMillis();

	private RedisURIClient targetRedisURIClient;

	protected <T extends RedisItemWriter<?, ?, ?>> T configure(T writer) {
		writer.setClient(targetRedisURIClient.getClient());
		return writer;
	}

	@Override
	protected RedisURIClient redisURIClient() {
		RedisClientBuilder builder = sourceRedisArgs.configure(redisClientBuilder());
		builder.uri(sourceRedisURI);
		log.info("Creating source Redis client with {}", builder);
		return builder.build();
	}

	private String compareMessage(Collection<StatusCount> counts) {
		StringBuilder builder = new StringBuilder();
		counts.stream().map(CompareStepListener::toString).forEach(s -> builder.append(STATUS_DELIMITER).append(s));
		return builder.toString();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (targetRedisURIClient == null) {
			RedisClientBuilder builder = targetRedisArgs.configure(redisClientBuilder());
			builder.uri(targetRedisURI);
			log.info("Creating target Redis client with {}", builder);
			targetRedisURIClient = builder.build();
		}
	}

	@Override
	protected void shutdown() {
		if (targetRedisURIClient != null) {
			log.info("Shutting down target Redis client");
			targetRedisURIClient.close();
			targetRedisURIClient = null;
		}
		super.shutdown();
	}

	@Override
	protected StandardEvaluationContext evaluationContext(EvaluationContextArgs args) {
		StandardEvaluationContext context = super.evaluationContext(args);
		log.info("Setting evaluation context variable {} = {}", SOURCE_VAR, client.getUri());
		context.setVariable(SOURCE_VAR, client.getUri());
		log.info("Setting evaluation context variable {} = {}", TARGET_VAR, targetRedisURIClient.getUri());
		context.setVariable(TARGET_VAR, targetRedisURIClient.getUri());
		return context;
	}

	protected Step<KeyComparison<byte[]>, KeyComparison<byte[]>> compareStep() {
		KeyComparisonItemReader<byte[], byte[]> reader = compareReader();
		CompareStatusItemWriter<byte[]> writer = new CompareStatusItemWriter<>();
		Step<KeyComparison<byte[]>, KeyComparison<byte[]>> step = new Step<>(reader, writer).name(COMPARE_STEP_NAME);
		step.taskName(COMPARE_TASK_NAME);
		step.statusMessageSupplier(() -> compareMessage(writer.getMismatches()));
		step.maxItemCountSupplier(RedisScanSizeEstimator.from(reader.getSourceReader()));
		if (showDiffs) {
			log.info("Adding key diff logger");
			step.writeListener(new CompareLoggingWriteListener<>(ByteArrayCodec.INSTANCE));
		}
		step.executionListener(new CompareStepListener(writer));
		return step;
	}

	private RedisItemReader<byte[], byte[], Object> compareRedisReader() {
		if (isQuickCompare()) {
			return RedisItemReader.type(ByteArrayCodec.INSTANCE);
		}
		return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
	}

	protected abstract boolean isQuickCompare();

	@Override
	protected <K, V, T> RedisItemReader<K, V, T> configure(RedisItemReader<K, V, T> reader) {
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
		return super.configure(reader);
	}

	protected KeyComparisonItemReader<byte[], byte[]> compareReader() {
		RedisItemReader<byte[], byte[], Object> source = compareSourceReader();
		RedisItemReader<byte[], byte[], Object> target = compareTargetReader();
		KeyComparisonItemReader<byte[], byte[]> reader = new KeyComparisonItemReader<>(source, target);
		reader.setComparator(keyComparator());
		return reader;
	}

	private KeyComparator<byte[]> keyComparator() {
		boolean ignoreStreamId = isIgnoreStreamMessageId();
		Duration ttlTolerance = Duration.ofMillis(ttlToleranceMillis);
		log.info("Creating KeyComparator with ttlTolerance={} ignoreStreamMessageId={}", ttlTolerance, ignoreStreamId);
		DefaultKeyComparator<byte[], byte[]> comparator = new DefaultKeyComparator<>(ByteArrayCodec.INSTANCE);
		comparator.setIgnoreStreamMessageId(ignoreStreamId);
		comparator.setTtlTolerance(ttlTolerance);
		return comparator;
	}

	protected abstract boolean isIgnoreStreamMessageId();

	private RedisItemReader<byte[], byte[], Object> compareSourceReader() {
		RedisItemReader<byte[], byte[], Object> reader = compareRedisReader();
		configure(reader);
		return reader;
	}

	private RedisItemReader<byte[], byte[], Object> compareTargetReader() {
		RedisItemReader<byte[], byte[], Object> reader = compareRedisReader();
		reader.setClient(targetRedisURIClient.getClient());
		reader.setDatabase(targetRedisURIClient.getUri().getDatabase());
		return reader;
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public TargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(TargetRedisArgs args) {
		this.targetRedisArgs = args;
	}

	public RedisURI getSourceRedisURI() {
		return sourceRedisURI;
	}

	public void setSourceRedisURI(RedisURI sourceRedisURI) {
		this.sourceRedisURI = sourceRedisURI;
	}

	public SourceRedisArgs getSourceRedisArgs() {
		return sourceRedisArgs;
	}

	public void setSourceRedisArgs(SourceRedisArgs sourceRedisArgs) {
		this.sourceRedisArgs = sourceRedisArgs;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

	public void setTargetRedisURI(RedisURI targetRedisURI) {
		this.targetRedisURI = targetRedisURI;
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

	public long getTtlToleranceMillis() {
		return ttlToleranceMillis;
	}

	public void setTtlToleranceMillis(long tolerance) {
		this.ttlToleranceMillis = tolerance;
	}

}
