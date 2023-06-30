package com.redis.riot.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.CompareStepListener;
import com.redis.riot.cli.common.NoopItemWriter;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.common.ReplicateCommandContext;
import com.redis.riot.cli.common.ReplicationOptions;
import com.redis.riot.cli.common.ReplicationStrategy;
import com.redis.riot.cli.common.StepProgressMonitor;
import com.redis.riot.core.KeyComparisonLogger;
import com.redis.riot.core.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.core.processor.KeyValueProcessor;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.LiveRedisItemReader.Builder;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractExportCommand {

	private static final Logger log = Logger.getLogger(Replicate.class.getName());

	private static final String COMPARE_MESSAGE = " %,d missing, %,d type, %,d value, %,d ttl";
	private static final String QUEUE_MESSAGE = " %,d queued notifications";
	private static final String JOB_NAME = "replicate-job";
	private static final String COMPARE_STEP = "compare-step";
	private static final String SCAN_STEP = "scan-step";
	private static final String LIVE_STEP = "live-step";
	private static final String TASK_SCAN = "Scanning";
	private static final String TASK_COMPARE = "Comparing";
	private static final String TASK_LIVE = "Listening";

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisOptions targetRedisOptions = new RedisOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();

	public RedisOptions getTargetRedisOptions() {
		return targetRedisOptions;
	}

	public void setTargetRedisOptions(RedisOptions targetRedisOptions) {
		this.targetRedisOptions = targetRedisOptions;
	}

	@Override
	protected CommandContext context(RedisURI redisURI, AbstractRedisClient redisClient) {
		RedisURI targetRedisURI = redisURI(targetRedisOptions);
		AbstractRedisClient targetRedisClient = client(targetRedisURI, targetRedisOptions);
		return new ReplicateCommandContext(redisURI, redisClient, targetRedisURI, targetRedisClient);
	}

	public ReplicationOptions getReplicateOptions() {
		return replicationOptions;
	}

	public void setReplicationOptions(ReplicationOptions replicationOptions) {
		this.replicationOptions = replicationOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(CommandContext jobCommandContext) {
		ReplicateCommandContext context = (ReplicateCommandContext) jobCommandContext;
		switch (replicationOptions.getMode()) {
		case COMPARE:
			return compareJob(context);
		case LIVE:
			return liveJob(context);
		case LIVEONLY:
			return liveOnlyJob(context);
		case SNAPSHOT:
			return snapshotJob(context);
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicationOptions.getMode());
		}
	}

	private Job liveOnlyJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = job(JOB_NAME).start(liveStep(context));
		return job.build();
	}

	private Job compareJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = job("compare-job").start(compareStep(context));
		return job.build();
	}

	private Job snapshotJob(ReplicateCommandContext context) {
		SimpleJobBuilder snapshotJob = job(JOB_NAME).start(scanStep(context));
		optionalCompareStep(context).ifPresent(snapshotJob::next);
		return snapshotJob.build();
	}

	private Job liveJob(ReplicateCommandContext context) {
		TaskletStep scanStep = scanStep(context);
		SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-flow").start(scanStep).build();
		TaskletStep liveStep = liveStep(context);
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-flow").start(liveStep).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replicate-flow").split(new SimpleAsyncTaskExecutor())
				.add(liveFlow, scanFlow).build();
		JobFlowBuilder liveJob = job(JOB_NAME).start(replicationFlow);
		optionalCompareStep(context).ifPresent(liveJob::next);
		return liveJob.build().build();
	}

	protected Optional<Step> optionalCompareStep(ReplicateCommandContext context) {
		if (replicationOptions.isNoVerify() || writerOptions.isDryRun()
				|| replicationOptions.getKeyProcessor().isPresent()) {
			return Optional.empty();
		}
		return Optional.of(compareStep(context));
	}

	private TaskletStep scanStep(ReplicateCommandContext context) {
		ScanBuilder scanBuilder = scanBuilder(context);
		RedisItemReader<byte[], byte[], ?> reader = build(scanBuilder);
		reader.setKeyProcessor(keyProcessor(ByteArrayCodec.INSTANCE));
		SimpleStepBuilder step = step(SCAN_STEP, reader, checkWriter(context));
		step.processor(processor(context));
		StepProgressMonitor monitor = monitor(TASK_SCAN, context);
		monitor.register(step);
		return step.build();
	}

	private RedisItemReader<byte[], byte[], ?> build(ScanBuilder readerBuilder) {
		if (isKeyDump()) {
			return readerBuilder.keyDump();
		}
		return readerBuilder.dataStructure(ByteArrayCodec.INSTANCE);
	}

	private TaskletStep liveStep(ReplicateCommandContext context) {
		LiveRedisItemReader.Builder builder = scanBuilder(context).live();
		builder.flushingOptions(replicationOptions.flushingOptions());
		builder.keyspaceNotificationOptions(keyspaceNotificationOptions(context));
		LiveRedisItemReader<byte[], byte[], ?> reader = build(builder);
		reader.setKeyProcessor(keyProcessor(ByteArrayCodec.INSTANCE));
		ItemWriter writer = checkWriter(context);
		FlushingStepBuilder step = new FlushingStepBuilder<>(step(LIVE_STEP, reader, writer));
		step.processor(processor(context));
		step.options(replicationOptions.flushingOptions());
		StepProgressMonitor monitor = monitor(TASK_LIVE);
		monitor.withExtraMessage(notificationQueueInfo(reader.getKeyReader()));
		monitor.register(step);
		return step.build();
	}

	private Supplier<String> notificationQueueInfo(KeyspaceNotificationItemReader reader) {
		return () -> String.format(Locale.US, QUEUE_MESSAGE, queueSize(reader.getQueue()));
	}

	private int queueSize(BlockingQueue<?> queue) {
		if (queue == null) {
			return 0;
		}
		return queue.size();
	}

	private LiveRedisItemReader<byte[], byte[], ?> build(Builder builder) {
		if (isKeyDump()) {
			return builder.keyDump();
		}
		return builder.dataStructure(ByteArrayCodec.INSTANCE);
	}

	private KeyspaceNotificationOptions keyspaceNotificationOptions(CommandContext context) {
		return KeyspaceNotificationOptions.builder().database(context.getRedisURI().getDatabase())
				.queueOptions(replicationOptions.notificationQueueOptions())
				.orderingStrategy(replicationOptions.getNotificationOrdering()).build();
	}

	protected Step compareStep(ReplicateCommandContext context) {
		log.log(Level.FINE, "Creating key comparator with TTL tolerance of {0} seconds",
				replicationOptions.getTtlTolerance());
		KeyComparisonItemReader.Builder comparator = new KeyComparisonItemReader.Builder(context.getRedisClient(),
				context.getTargetRedisClient());
		configureScanBuilder(comparator);
		comparator.ttlTolerance(Duration.ofMillis(replicationOptions.getTtlTolerance()));
		comparator.rightOptions(replicationOptions.targetReaderOptions());
		KeyComparisonItemReader reader = comparator.build();
		reader.getLeft().setKeyProcessor(keyProcessor());
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		SimpleStepBuilder<KeyComparison, KeyComparison> step = step(COMPARE_STEP, reader, writer);
		StepProgressMonitor monitor = monitor(TASK_COMPARE, context);
		monitor.withExtraMessage(() -> extraMessage(writer.getResults()));
		monitor.register(step);
		if (replicationOptions.isShowDiffs()) {
			step.listener(new KeyComparisonLogger(log));
		}
		step.listener(new CompareStepListener(writer));
		return step.build();
	}

	private String extraMessage(Results results) {
		return String.format(Locale.US, COMPARE_MESSAGE, results.getCount(Status.MISSING),
				results.getCount(Status.TYPE), results.getCount(Status.VALUE), results.getCount(Status.TTL));
	}

	private ItemWriter checkWriter(ReplicateCommandContext context) {
		if (writerOptions.isDryRun()) {
			return new NoopItemWriter<>();
		}
		WriterBuilder writer = writer(context.getTargetRedisClient(), writerOptions);
		return build(writer);
	}

	private ItemWriter build(WriterBuilder writer) {
		if (isKeyDump()) {
			return writer.keyDump();
		}
		return writer.dataStructure(ByteArrayCodec.INSTANCE);
	}

	private boolean isKeyDump() {
		return replicationOptions.getStrategy() == ReplicationStrategy.DUMP;
	}

	private ItemProcessor<byte[], ?> processor(ReplicateCommandContext context) {
		SpelExpressionParser parser = new SpelExpressionParser();
		List<ItemProcessor> processors = new ArrayList<>();
		replicationOptions.getKeyProcessor().ifPresent(p -> {
			EvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("src", context.getRedisURI());
			evaluationContext.setVariable("dest", context.getTargetRedisURI());
			Expression expression = parser.parseExpression(p);
			processors.add(new KeyValueProcessor<>(expression, evaluationContext));
		});
		return CompositeItemStreamItemProcessor.delegates(processors.toArray(new ItemProcessor[0]));
	}

}
