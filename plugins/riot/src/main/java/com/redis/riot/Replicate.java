package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.function.StringKeyValue;
import com.redis.riot.function.ToStringKeyValue;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyNotificationStatus;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractTargetCommand {

	public enum CompareMode {
		FULL, QUICK, NONE
	}

	public static final String STEP_NAME = "replicate";
	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;

	private static final String QUEUE_MESSAGE = " | capacity: %,d | dropped: %,d";
	private static final String SCAN_TASK_NAME = "Scanning";
	private static final String LIVEONLY_TASK_NAME = "Listening";
	private static final String LIVE_TASK_NAME = "Scanning/Listening";

	@Option(names = "--struct", description = "Enable data structure-specific replication")
	private boolean struct;

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ProcessorArgs processorArgs = new ProcessorArgs();

	@ArgGroup(exclusive = false)
	private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

	@Option(names = "--log-keys", description = "Log keys being read and written.")
	private boolean logKeys;

	@Option(names = "--compare", description = "Compare mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
	private CompareMode compareMode = DEFAULT_COMPARE_MODE;

	@Override
	protected boolean isQuickCompare() {
		return compareMode == CompareMode.QUICK;
	}

	@Override
	protected Job job() {
		List<Step<?, ?>> steps = new ArrayList<>();
		Step<KeyValue<byte[], Object>, KeyValue<byte[], Object>> replicateStep = replicateStep();
		steps.add(replicateStep);
		if (shouldCompare()) {
			steps.add(compareStep());
		}
		return job(steps);
	}

	@Override
	protected boolean isIgnoreStreamMessageId() {
		return !processorArgs.isPropagateStreamMessageId();
	}

	private ItemProcessor<KeyValue<byte[], Object>, KeyValue<byte[], Object>> processor() {
		return RiotUtils.processor(new MemKeyValueFilter<>(ByteArrayCodec.INSTANCE, log), keyValueProcessor());
	}

	private ItemProcessor<KeyValue<byte[], Object>, KeyValue<byte[], Object>> keyValueProcessor() {
		ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> processor = processorArgs
				.keyValueProcessor(evaluationContext(processorArgs));
		if (processor == null) {
			return null;
		}
		ToStringKeyValue<byte[]> code = new ToStringKeyValue<>(ByteArrayCodec.INSTANCE);
		StringKeyValue<byte[]> decode = new StringKeyValue<>(ByteArrayCodec.INSTANCE);
		return RiotUtils.processor(new FunctionItemProcessor<>(code), processor, new FunctionItemProcessor<>(decode));
	}

	@Override
	protected <T extends RedisItemWriter<?, ?, ?>> T configure(T writer) {
		log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
		targetRedisWriterArgs.configure(writer);
		return super.configure(writer);
	}

	private Step<KeyValue<byte[], Object>, KeyValue<byte[], Object>> replicateStep() {
		RedisItemReader<byte[], byte[], Object> reader = configure(sourceReader());
		RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer = configure(writer());
		Step<KeyValue<byte[], Object>, KeyValue<byte[], Object>> step = new Step<>(STEP_NAME, reader, writer);
		step.processor(processor());
		step.taskName(taskName(reader));
		configureExportStep(step);
		if (reader.getMode() != ReaderMode.SCAN) {
			step.statusMessageSupplier(() -> liveExtraMessage(reader));
		}
		step.maxItemCountSupplier(RedisScanSizeEstimator.from(reader));
		if (logKeys) {
			log.info("Adding key logger");
			ReplicateWriteLogger<byte[], Object> writeLogger = new ReplicateWriteLogger<>(log, reader.getCodec());
			step.writeListener(writeLogger);
			ReplicateReadLogger<byte[]> readLogger = new ReplicateReadLogger<>(log, reader.getCodec());
			reader.addItemReadListener(readLogger);
			reader.addItemWriteListener(readLogger);
		}
		return step;
	}

	private boolean shouldCompare() {
		return compareMode != CompareMode.NONE && !getJobArgs().isDryRun();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemReader<byte[], byte[], Object> sourceReader() {
		if (struct) {
			log.info("Creating data-structure type Redis reader");
			return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		}
		log.info("Creating dump Redis reader");
		return (RedisItemReader) RedisItemReader.dump();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemWriter<byte[], byte[], KeyValue<byte[], Object>> writer() {
		if (struct) {
			return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		}
		return (RedisItemWriter) RedisItemWriter.dump();
	}

	private String taskName(RedisItemReader<?, ?, ?> reader) {
		switch (reader.getMode()) {
		case SCAN:
			return SCAN_TASK_NAME;
		case LIVEONLY:
			return LIVEONLY_TASK_NAME;
		default:
			return LIVE_TASK_NAME;
		}
	}

	private String liveExtraMessage(RedisItemReader<?, ?, ?> reader) {
		KeyNotificationItemReader<?, ?> keyReader = (KeyNotificationItemReader<?, ?>) reader.getReader();
		if (keyReader == null || keyReader.getQueue() == null) {
			return "";
		}
		return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity(),
				keyReader.count(KeyNotificationStatus.DROPPED));
	}

	@Override
	protected KeyComparisonItemReader<byte[], byte[]> compareReader() {
		KeyComparisonItemReader<byte[], byte[]> reader = super.compareReader();
		reader.setProcessor(processor());
		return reader;
	}

	public RedisWriterArgs getTargetRedisWriterArgs() {
		return targetRedisWriterArgs;
	}

	public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.targetRedisWriterArgs = redisWriterArgs;
	}

	public boolean isStruct() {
		return struct;
	}

	public void setStruct(boolean type) {
		this.struct = type;
	}

	public ProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(ProcessorArgs args) {
		this.processorArgs = args;
	}

	public boolean isLogKeys() {
		return logKeys;
	}

	public void setLogKeys(boolean enable) {
		this.logKeys = enable;
	}

	public CompareMode getCompareMode() {
		return compareMode;
	}

	public void setCompareMode(CompareMode compareMode) {
		this.compareMode = compareMode;
	}

}
