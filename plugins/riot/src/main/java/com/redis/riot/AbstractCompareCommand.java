package com.redis.riot;

import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.riot.core.ProcessingItemWriter;
import com.redis.riot.core.RiotDuration;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.function.StringKeyValue;
import com.redis.riot.function.ToStringKeyValue;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemWriter;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStat;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStats;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractCompareCommand extends AbstractRedisTargetExportCommand {

	public static final RiotDuration DEFAULT_TTL_TOLERANCE = RiotDuration.of(DefaultKeyComparator.DEFAULT_TTL_TOLERANCE,
			ChronoUnit.SECONDS);
	public static final boolean DEFAULT_COMPARE_STREAM_MESSAGE_ID = true;

	private static final String COMPARE_TASK_NAME = "Comparing";

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
	private boolean showDiffs;

	@Option(names = "--ttl-tolerance", description = "Max TTL delta to consider keys equal (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
	private RiotDuration ttlTolerance = DEFAULT_TTL_TOLERANCE;

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	protected abstract boolean isStruct();

	protected ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> filter() {
		return new KeyValueFilter<>(ByteArrayCodec.INSTANCE);
	}

	protected ItemWriter<KeyValue<byte[]>> processingWriter(ItemWriter<KeyValue<byte[]>> writer) {
		if (isIgnoreStreamMessageId()) {
			Assert.isTrue(isStruct(), "--no-stream-id can only be used with --struct");
		}
		StandardEvaluationContext evaluationContext = evaluationContext();
		log.info("Creating processor with {}", processorArgs);
		ItemProcessor<KeyValue<String>, KeyValue<String>> processor = processorArgs.processor(evaluationContext);
		if (processor == null) {
			return writer;
		}
		ToStringKeyValue<byte[]> code = new ToStringKeyValue<>(ByteArrayCodec.INSTANCE);
		StringKeyValue<byte[]> decode = new StringKeyValue<>(ByteArrayCodec.INSTANCE);
		ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> finalProcessor = RiotUtils
				.processor(new FunctionItemProcessor<>(code), processor, new FunctionItemProcessor<>(decode));
		return new ProcessingItemWriter<>(finalProcessor, writer);
	}

	private StandardEvaluationContext evaluationContext() {
		log.info("Creating SpEL evaluation context with {}", evaluationContextArgs);
		StandardEvaluationContext evaluationContext = evaluationContextArgs.evaluationContext();
		configure(evaluationContext);
		return evaluationContext;
	}

	private String compareMessage(KeyComparisonStats stats) {
		return CompareStepListener.statsByStatus(stats).stream()
				.map(e -> String.format("%s %d", e.getKey(),
						e.getValue().stream().collect(Collectors.summingLong(KeyComparisonStat::getCount))))
				.collect(Collectors.joining(" | "));
	}

	protected Step<KeyValue<byte[]>, KeyValue<byte[]>> compareStep() {
		RedisItemReader<byte[], byte[]> sourceReader = compareSourceReader();
		RedisItemReader<byte[], byte[]> targetReader = compareTargetReader();
		KeyComparisonItemWriter<byte[], byte[]> writer = new KeyComparisonItemWriter<>(targetReader, keyComparator());
		if (showDiffs) {
			log.info("Adding key diff logger");
			writer.addListener(new CompareLoggingWriteListener<>(ByteArrayCodec.INSTANCE));
		}
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = new Step<>(sourceReader, processingWriter(writer));
		step.processor(filter());
		step.taskName(COMPARE_TASK_NAME);
		step.statusMessageSupplier(() -> compareMessage(writer.getStats()));
		step.maxItemCountSupplier(RedisScanSizeEstimator.from(sourceReader));
		step.executionListener(new CompareStepListener(writer.getStats()));
		return step;
	}

	private RedisItemReader<byte[], byte[]> compareRedisReader() {
		if (isQuickCompare()) {
			log.info("Creating Redis quick compare reader");
			return RedisItemReader.type(ByteArrayCodec.INSTANCE);
		}
		log.info("Creating Redis full compare reader");
		return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
	}

	protected abstract boolean isQuickCompare();

	private KeyComparator<byte[]> keyComparator() {
		boolean ignoreStreamId = isIgnoreStreamMessageId();
		log.info("Creating KeyComparator with ttlTolerance={} ignoreStreamMessageId={}", ttlTolerance, ignoreStreamId);
		DefaultKeyComparator<byte[], byte[]> comparator = new DefaultKeyComparator<>(ByteArrayCodec.INSTANCE);
		comparator.setIgnoreStreamMessageId(ignoreStreamId);
		comparator.setTtlTolerance(ttlTolerance.getValue());
		return comparator;
	}

	protected boolean isIgnoreStreamMessageId() {
		return processorArgs.isNoStreamIds();
	}

	private RedisItemReader<byte[], byte[]> compareSourceReader() {
		RedisItemReader<byte[], byte[]> reader = compareRedisReader();
		configureSourceRedisReader(reader);
		return reader;
	}

	private RedisItemReader<byte[], byte[]> compareTargetReader() {
		RedisItemReader<byte[], byte[]> reader = compareRedisReader();
		configureTargetRedisReader(reader);
		return reader;
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

	public RiotDuration getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(RiotDuration tolerance) {
		this.ttlTolerance = tolerance;
	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs args) {
		this.processorArgs = args;
	}

}
