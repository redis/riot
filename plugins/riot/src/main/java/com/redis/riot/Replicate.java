package com.redis.riot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.function.StringKeyValueFunction;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractTargetExport {

	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;
	public static final Duration DEFAULT_TTL_TOLERANCE = DefaultKeyComparator.DEFAULT_TTL_TOLERANCE;
	public static final String STEP_REPLICATE = "replicate-step";
	public static final String STEP_COMPARE = "compare-step";

	private static final String QUEUE_MESSAGE = " | queue capacity: %,d";
	private static final String TASK_SCAN = "Scanning";
	private static final String TASK_LIVEONLY = "Listening";
	private static final String TASK_LIVE = "Scanning & listening";

	@Option(names = "--struct", description = "Enable data-structure type-based replication")
	private boolean struct;

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ReplicateProcessorArgs processorArgs = new ReplicateProcessorArgs();

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@Option(names = "--log-writes", description = "Log written keys.")
	private boolean logWrittenKeys;

	@ArgGroup(exclusive = false)
	private CompareArgs compareArgs = new CompareArgs();

	public void copyTo(Replicate target) {
		super.copyTo(target);
		target.struct = struct;
		target.processorArgs = processorArgs;
		target.redisWriterArgs = redisWriterArgs;
		target.logWrittenKeys = logWrittenKeys;
		target.compareArgs = compareArgs;
	}

	@Override
	protected Job job() {
		List<Step<?, ?>> steps = new ArrayList<>();
		Step<KeyValue<byte[], ?>, KeyValue<byte[], ?>> replicateStep = step();
		ItemProcessor<KeyValue<byte[], ?>, KeyValue<byte[], ?>> processor = processor();
		replicateStep.processor(processor);
		steps.add(replicateStep);
		if (shouldCompare() && processor == null) {
			Compare compareCommand = new Compare();
			compareCommand
					.setCompareStreamMessageId(processorArgs.getKeyValueProcessorArgs().isPropagateStreamMessageIds());
			copyTo(compareCommand);
			steps.add(compareCommand.compareStep());
		}
		return job(steps);
	}

	protected ItemProcessor<KeyValue<byte[], ?>, KeyValue<byte[], ?>> processor() {
		StandardEvaluationContext evaluationContext = processorArgs.getEvaluationContextArgs().evaluationContext();
		configure(evaluationContext);
		ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor = processorArgs
				.getKeyValueProcessorArgs().processor(evaluationContext);
		if (processor == null) {
			return null;
		}
		ItemProcessor<KeyValue<byte[], Object>, KeyValue<String, Object>> code = new ToStringKeyValueProcessor<>(
				ByteArrayCodec.INSTANCE);
		ItemProcessor<KeyValue<String, Object>, KeyValue<byte[], Object>> decode = new StringKeyValueFunction<>(
				ByteArrayCodec.INSTANCE);
		return RiotUtils.processor(code, processor, decode);
	}

	private Step<KeyValue<byte[], ?>, KeyValue<byte[], ?>> step() {
		RedisItemReader<byte[], byte[], KeyValue<byte[], ?>> reader = reader();
		configure(reader);
		RedisItemWriter<byte[], byte[], KeyValue<byte[], ?>> writer = writer();
		configure(writer);
		Step<KeyValue<byte[], ?>, KeyValue<byte[], ?>> step = exportStep(reader, writer).name(STEP_REPLICATE);
		step.taskName(taskName(reader.getMode()));
		if (reader.getMode() != ReaderMode.SCAN) {
			step.statusMessageSupplier(() -> liveExtraMessage(reader));
		}
		step.maxItemCountSupplier(scanSizeEstimator(reader));
		if (logWrittenKeys) {
			log.info("Adding key-write logger");
			step.writeListener(new LoggingKeyValueWriteListener());
		}
		return step;
	}

	private boolean shouldCompare() {
		return compareArgs.getMode() != CompareMode.NONE && !getJobArgs().isDryRun();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemReader<byte[], byte[], KeyValue<byte[], ?>> reader() {
		if (struct) {
			log.info("Creating data-structure type Redis reader");
			return (RedisItemReader) RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		}
		log.info("Creating dump Redis reader");
		return (RedisItemReader) RedisItemReader.dump();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemWriter<byte[], byte[], KeyValue<byte[], ?>> writer() {
		if (struct) {
			return (RedisItemWriter) RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		}
		return (RedisItemWriter) RedisItemWriter.dump();
	}

	private String taskName(ReaderMode mode) {
		switch (mode) {
		case SCAN:
			return TASK_SCAN;
		case LIVEONLY:
			return TASK_LIVEONLY;
		default:
			return TASK_LIVE;
		}
	}

	private String liveExtraMessage(RedisItemReader<?, ?, ?> reader) {
		KeyNotificationItemReader<?, ?> keyReader = (KeyNotificationItemReader<?, ?>) reader.getReader();
		if (keyReader == null || keyReader.getQueue() == null) {
			return "";
		}
		return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity());
	}

	@Override
	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		configureTarget(writer);
		log.info("Configuring target Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.redisWriterArgs = redisWriterArgs;
	}

	public boolean isStruct() {
		return struct;
	}

	public void setStruct(boolean type) {
		this.struct = type;
	}

	public CompareArgs getCompareArgs() {
		return compareArgs;
	}

	public void setCompareArgs(CompareArgs args) {
		this.compareArgs = args;
	}

	public ReplicateProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(ReplicateProcessorArgs processorArgs) {
		this.processorArgs = processorArgs;
	}

}
