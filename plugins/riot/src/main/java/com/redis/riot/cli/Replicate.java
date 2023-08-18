package com.redis.riot.cli;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import com.redis.riot.core.processor.IgnoreKeysWithTTLProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.NoopItemWriter;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RedisStructOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.common.ReplicateCommandContext;
import com.redis.riot.cli.common.ReplicateOptions;
import com.redis.riot.cli.common.ReplicationStrategy;
import com.redis.riot.cli.common.RiotStep;
import com.redis.riot.cli.common.RiotUtils;
import com.redis.riot.cli.common.JobOptions.ProgressStyle;
import com.redis.riot.core.CompareStepExecutionListener;
import com.redis.riot.core.KeyComparisonDiffLogger;
import com.redis.riot.core.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.core.processor.KeyValueProcessor;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.Builder;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.ValueType;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractExportCommand {

	private static final String COMPARE_MESSAGE = " | {0} missing | {1} type | {2} value | {3} ttl";
	private static final String QUEUE_MESSAGE = " | {0} queued";
	private static final String JOB_NAME = "replicate";
	private static final String COMPARE_STEP = "compare";
	private static final String SCAN_STEP = "scan";
	private static final String LIVE_STEP = "live";
	private static final String TASK_SCAN = "Scanning";
	private static final String TASK_COMPARE = "Comparing";
	private static final String TASK_LIVE = "Listening";

	private static final String READER_SUFFIX = "-reader";

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisOptions targetRedisOptions = new RedisOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@ArgGroup(exclusive = false, heading = "Data structure options%n")
	private RedisStructOptions structOptions = new RedisStructOptions();

	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions replicateOptions = new ReplicateOptions();

	public RedisOptions getTargetRedisOptions() {
		return targetRedisOptions;
	}

	public void setTargetRedisOptions(RedisOptions targetRedisOptions) {
		this.targetRedisOptions = targetRedisOptions;
	}

	@Override
	protected CommandContext context(RedisURI redisURI, AbstractRedisClient redisClient) {
		RedisURI targetRedisURI = RiotUtils.redisURI(targetRedisOptions);
		AbstractRedisClient targetRedisClient = RiotUtils.client(targetRedisURI, targetRedisOptions);
		return new ReplicateCommandContext(redisURI, redisClient, targetRedisURI, targetRedisClient);
	}

	public ReplicateOptions getReplicateOptions() {
		return replicateOptions;
	}

	public void setReplicateOptions(ReplicateOptions replicationOptions) {
		this.replicateOptions = replicationOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(CommandContext context) {
		ReplicateCommandContext replicateContext = (ReplicateCommandContext) context;
		switch (replicateOptions.getMode()) {
		case COMPARE:
			return compareJob(replicateContext);
		case LIVE:
			return liveJob(replicateContext);
		case LIVEONLY:
			return liveOnlyJob(replicateContext);
		case SNAPSHOT:
			return snapshotJob(replicateContext);
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicateOptions.getMode());
		}
	}

	private Job liveOnlyJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = job(JOB_NAME).start(liveStep(context).build());
		return job.build();
	}

	private Job compareJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = job("compare-job").start(compareStep(context).build());
		return job.build();
	}

	private Job snapshotJob(ReplicateCommandContext context) {
		SimpleJobBuilder snapshotJob = job(JOB_NAME).start(scanStep(context).build());
		if (shouldCompare()) {
			snapshotJob.next(compareStep(context).build());
		}
		return snapshotJob.build();
	}

	private Job liveJob(ReplicateCommandContext context) {
		SimpleStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep = scanStep(context);
		SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-flow").start(scanStep.build()).build();
		FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep = liveStep(context);
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-flow").start(liveStep.build()).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replicate-flow").split(new SimpleAsyncTaskExecutor())
				.add(liveFlow, scanFlow).build();
		JobFlowBuilder liveJob = job(JOB_NAME).start(replicationFlow);
		if (shouldCompare()) {
			liveJob.next(compareStep(context).build());
		}
		return liveJob.build().build();
	}

	private boolean shouldCompare() {
		return !replicateOptions.isNoVerify() && !writerOptions.isDryRun()
				&& !replicateOptions.getKeyProcessor().isPresent()
				&& !replicateOptions.isIgnoreKeysWithTtl();
	}

	private SimpleStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep(ReplicateCommandContext context) {
		RedisItemReader<byte[], byte[]> reader = reader(context, ByteArrayCodec.INSTANCE).build(valueType());
		reader.setName(SCAN_STEP + READER_SUFFIX);
		RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> step = step(reader, checkWriter(context)).name(SCAN_STEP);
		step.task(TASK_SCAN);
		step.processor(processor(context));
		return step.build();
	}

	private ValueType valueType() {
		if (replicateOptions.getStrategy() == ReplicationStrategy.DUMP) {
			return ValueType.DUMP;
		}
		return ValueType.STRUCT;
	}

	private FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep(ReplicateCommandContext context) {
		LiveRedisItemReader.Builder<byte[], byte[]> readerBuilder = reader(context, ByteArrayCodec.INSTANCE).live();
		readerBuilder.flushingOptions(replicateOptions.flushingOptions());
		readerBuilder.keyspaceNotificationOptions(keyspaceNotificationOptions(context));
		LiveRedisItemReader<byte[], byte[]> reader = readerBuilder.build(valueType());
		reader.setName(LIVE_STEP + READER_SUFFIX);
		RiotStep<KeyValue<byte[]>, KeyValue<byte[]>> riotStep = step(reader, checkWriter(context)).name(LIVE_STEP);
		riotStep.task(TASK_LIVE);
		riotStep.processor(processor(context));
		riotStep.extraMessage(() -> MessageFormat.format(QUEUE_MESSAGE, queueSize(reader.getKeyReader().getQueue())));
		FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = new FlushingStepBuilder<>(riotStep.build());
		step.options(replicateOptions.flushingOptions());
		return step;
	}

	private int queueSize(BlockingQueue<?> queue) {
		if (queue == null) {
			return 0;
		}
		return queue.size();
	}

	private KeyspaceNotificationOptions keyspaceNotificationOptions(CommandContext context) {
		return KeyspaceNotificationOptions.builder().database(context.getRedisURI().getDatabase())
				.queueOptions(replicateOptions.notificationQueueOptions())
				.orderingStrategy(replicateOptions.getNotificationOrdering()).build();
	}

	private SimpleStepBuilder<KeyComparison, KeyComparison> compareStep(ReplicateCommandContext context) {
		KeyComparisonItemReader.Builder comparator = KeyComparisonItemReader.builder(context.getRedisClient(),
				context.getTargetRedisClient());
		configure(comparator, StringCodec.UTF8);
		comparator.rightOptions(replicateOptions.targetReaderOptions());
		comparator.ttlTolerance(Duration.ofMillis(replicateOptions.getTtlTolerance()));
		KeyComparisonItemReader reader = comparator.build();
		reader.setName(COMPARE_STEP + READER_SUFFIX);
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		RiotStep<KeyComparison, KeyComparison> riotStep = step(reader, writer).name(COMPARE_STEP).task(TASK_COMPARE);
		riotStep.extraMessage(() -> extraMessage(writer.getResults()));
		if (replicateOptions.isShowDiffs()) {
			riotStep.progressStyle(ProgressStyle.LOG);
		}
		SimpleStepBuilder<KeyComparison, KeyComparison> step = riotStep.build();
		Logger logger = Logger.getLogger(getClass().getName());
		if (replicateOptions.isShowDiffs()) {
			step.listener(new KeyComparisonDiffLogger(logger));
		}
		step.listener(new CompareStepExecutionListener(writer, logger));
		return step;
	}

	private String extraMessage(Results results) {
		return MessageFormat.format(COMPARE_MESSAGE, results.getCount(Status.MISSING), results.getCount(Status.TYPE),
				results.getCount(Status.VALUE), results.getCount(Status.TTL));
	}

	private ItemWriter<KeyValue<byte[]>> checkWriter(ReplicateCommandContext context) {
		if (writerOptions.isDryRun()) {
			return new NoopItemWriter<>();
		}
		Builder<byte[], byte[]> writer = RedisItemWriter.client(context.getTargetRedisClient(),
				ByteArrayCodec.INSTANCE);
		writer.options(writerOptions.writerOptions());
		writer.structOptions(structOptions.structOptions());
		return writer.build(valueType());
	}

	private ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> processor(ReplicateCommandContext context) {
		SpelExpressionParser parser = new SpelExpressionParser();
		List<ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>>> processors = new ArrayList<>();
		replicateOptions.getKeyProcessor().ifPresent(p -> {
			EvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("src", context.getRedisURI());
			evaluationContext.setVariable("dest", context.getTargetRedisURI());
			Expression expression = parser.parseExpression(p);
			processors.add(new KeyValueProcessor(expression, evaluationContext));
		});
		if (replicateOptions.isIgnoreKeysWithTtl()) {
			processors.add(new IgnoreKeysWithTTLProcessor());
		}
		return CompositeItemStreamItemProcessor.delegates(processors.toArray(new ItemProcessor[0]));
	}

	@Override
	public String toString() {
		return "Replicate [targetRedisOptions=" + targetRedisOptions + ", writerOptions=" + writerOptions
				+ ", structOptions=" + structOptions + ", replicateOptions=" + replicateOptions + ", readerOptions="
				+ readerOptions + ", jobOptions=" + jobOptions + "]";
	}

}
