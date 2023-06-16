package com.redis.riot.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Predicate;
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

import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.KeyComparisonStepListener;
import com.redis.riot.cli.common.KeyComparisonWriteListener;
import com.redis.riot.cli.common.NoopItemWriter;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RedisReaderOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.common.ReplicateCommandContext;
import com.redis.riot.cli.common.ReplicationOptions;
import com.redis.riot.cli.common.ReplicationStrategy;
import com.redis.riot.cli.common.StepProgressMonitor;
import com.redis.riot.core.KeyComparisonLogger;
import com.redis.riot.core.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.core.processor.KeyValueProcessor;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.AbstractReaderBuilder;
import com.redis.spring.batch.RedisItemReader.ComparatorBuilder;
import com.redis.spring.batch.RedisItemReader.LiveReaderBuilder;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.SlotRangeFilter;
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
public class Replicate extends AbstractCommand {

	private static final Logger log = Logger.getLogger(Replicate.class.getName());

	private static final String COMPARE_MESSAGE = " >%,d T%,d ≠%,d ⧗%,d";

	private static final String JOB_NAME = "replicate-job";

	private static final String COMPARE_STEP = "compare-step";

	private static final String SCAN_STEP = "scan-step";

	private static final String LIVE_STEP = "live-step";

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisOptions targetRedisOptions = new RedisOptions();

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

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

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	@Override
	protected CommandContext context(RedisURI redisURI, AbstractRedisClient redisClient) {
		RedisURI targetRedisURI = redisURI(targetRedisOptions);
		AbstractRedisClient targetRedisClient = client(targetRedisURI, targetRedisOptions);
		return new ReplicateCommandContext(redisURI, redisClient, targetRedisURI, targetRedisClient);
	}

	protected Step compareStep(ReplicateCommandContext context) {
		log.log(Level.FINE, "Creating key comparator with TTL tolerance of {0} seconds",
				replicationOptions.getTtlTolerance());
		ComparatorBuilder comparator = new ComparatorBuilder(context.getRedisClient(), context.getTargetRedisClient());
		configureReader(comparator, readerOptions);
		comparator.scanCount(readerOptions.getScanCount());
		comparator.scanMatch(readerOptions.getScanMatch());
		comparator.scanType(readerOptions.getScanType());
		comparator.ttlTolerance(Duration.ofMillis(replicationOptions.getTtlTolerance()));
		comparator.rightPoolOptions(PoolOptions.builder().maxTotal(replicationOptions.getTargetPoolMaxTotal()).build());
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		SimpleStepBuilder<KeyComparison, KeyComparison> step = step(COMPARE_STEP);
		RedisItemReader<String, String, KeyComparison> reader = comparator.build();
		reader.setName(readerName(COMPARE_STEP));
		step.reader(reader);
		step.writer(writer);
		if (replicationOptions.isShowDiffs()) {
			step.listener(new KeyComparisonWriteListener(new KeyComparisonLogger(log)));
		}
		step.listener(new KeyComparisonStepListener(writer, getTransferOptions().getProgressUpdateInterval()));
		StepProgressMonitor monitor = progressMonitor("Comparing")
				.withInitialMax(estimator(context.getRedisClient(), readerOptions))
				.withExtraMessage(() -> extraMessage(writer.getResults()));
		monitor.register(step);
		return step.build();
	}

	private String extraMessage(Results results) {
		return String.format(Locale.US, COMPARE_MESSAGE, results.getCount(Status.MISSING),
				results.getCount(Status.TYPE), results.getCount(Status.VALUE), results.getCount(Status.TTL));
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
		if (replicationOptions.isNoVerify()) {
			return Optional.empty();
		}
		if (writerOptions.isDryRun()) {
			return Optional.empty();
		}
		if (replicationOptions.getKeyProcessor().isPresent()) {
			// Verification cannot be done if a processor is set
			log.warning("Key processor enabled, verification will be skipped");
			return Optional.empty();
		}
		return Optional.of(compareStep(context));
	}

	private TaskletStep scanStep(ReplicateCommandContext context) {
		SimpleStepBuilder step = step(SCAN_STEP);
		RedisItemReader reader = reader(reader(context.getRedisClient(), readerOptions));
		reader.setName(readerName(SCAN_STEP));
		step.reader(reader);
		ItemWriter writer = checkWriter(context);
		step.writer(writer);
		ItemProcessor processor = processor(context);
		step.processor(processor);
		StepProgressMonitor monitor = progressMonitor("Scanning");
		monitor.withInitialMax(estimator(context.getRedisClient(), readerOptions));
		monitor.register(step);
		return step.build();
	}

	private TaskletStep liveStep(ReplicateCommandContext context) {
		SimpleStepBuilder step = configureStep(flushingStep());
		LiveRedisItemReader reader = liveReader(context);
		reader.setName(readerName(LIVE_STEP));
		step.reader(reader);
		ItemProcessor<byte[], ?> processor = processor(context);
		step.processor(processor);
		ItemWriter writer = checkWriter(context);
		step.writer(writer);
		StepProgressMonitor monitor = progressMonitor("Listening");
		monitor.register(step);
		return step.build();
	}

	private SimpleStepBuilder flushingStep() {
		FlushingStepBuilder step = new FlushingStepBuilder<>(stepBuilder(LIVE_STEP));
		step.flushingInterval(Duration.ofMillis(replicationOptions.getFlushInterval()));
		step.idleTimeout(Duration.ofMillis(replicationOptions.getIdleTimeout()));
		return step;
	}

	private String readerName(String name) {
		return name + "-reader";
	}

	private LiveRedisItemReader<byte[], byte[], ?> liveReader(CommandContext context) {
		LiveReaderBuilder builder = reader(context.getRedisClient(), readerOptions).live();
		builder.flushingInterval(Duration.ofMillis(replicationOptions.getFlushInterval()));
		builder.idleTimeout(Duration.ofMillis(replicationOptions.getIdleTimeout()));
		builder.database(context.getRedisURI().getDatabase());
		builder.notificationQueueOptions(replicationOptions.notificationQueueOptions());
		builder.notificationOrdering(replicationOptions.getNotificationOrdering());
		LiveRedisItemReader<byte[], byte[], ?> reader = (LiveRedisItemReader<byte[], byte[], ?>) reader(builder);
		replicationOptions.getKeySlot().map(this::keySlotFilter)
				.ifPresent(f -> reader.withKeyProcessor(new FilteringItemProcessor<>(f)));
		return reader;
	}

	private ItemWriter checkWriter(ReplicateCommandContext context) {
		if (writerOptions.isDryRun()) {
			return new NoopItemWriter<>();
		}
		WriterBuilder writer = writer(context.getTargetRedisClient(), writerOptions);
		return isKeyDump() ? writer.keyDump() : writer.dataStructure(ByteArrayCodec.INSTANCE);
	}

	private boolean isKeyDump() {
		return replicationOptions.getStrategy() == ReplicationStrategy.DUMP;
	}

	private <B extends AbstractReaderBuilder<B>> RedisItemReader<byte[], byte[], ?> reader(B reader) {
		return isKeyDump() ? reader.keyDump() : reader.dataStructure(ByteArrayCodec.INSTANCE);
	}

	private Predicate<byte[]> keySlotFilter(IntRange range) {
		return SlotRangeFilter.of(ByteArrayCodec.INSTANCE, range.getMin(), range.getMax());
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
