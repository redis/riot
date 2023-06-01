package com.redis.riot.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.KeyComparisonStepListener;
import com.redis.riot.cli.common.KeyComparisonWriteListener;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RedisReaderOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.common.ReplicateCommandContext;
import com.redis.riot.cli.common.ReplicateOptions;
import com.redis.riot.cli.common.ReplicateOptions.ReplicateStrategy;
import com.redis.riot.core.KeyComparisonLogger;
import com.redis.riot.core.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.core.processor.KeyValueProcessor;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ComparatorBuilder;
import com.redis.spring.batch.RedisItemReader.LiveBuilder;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.AbstractBuilder;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.SlotRangeFilter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.operation.Noop;

import io.lettuce.core.codec.ByteArrayCodec;
import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractCommand {

	private static final Logger log = Logger.getLogger(Replicate.class.getName());

	private static final String COMPARE_MESSAGE_ASCII = " >%,d T%,d ≠%,d ⧗%,d <%,d";
	private static final String COMPARE_MESSAGE_COLOR = " \u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d\u001b[0m";
	private static final Noop<byte[], byte[], KeyValue<byte[]>, Object> NOOP_OPERATION = new Noop<>();

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisOptions targetRedisOptions = new RedisOptions();

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Mixin
	private ReplicateOptions replicateOptions = new ReplicateOptions();

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
	protected CommandContext context(JobRunner jobRunner, RedisOptions redisOptions) {
		return new ReplicateCommandContext(jobRunner, redisOptions, targetRedisOptions);
	}

	protected ScanSizeEstimator estimator(CommandContext context) {
		return new ScanSizeEstimator(context.getRedisClient(), readerOptions.scanSizeEstimatorOptions());
	}

	protected Step verificationStep(ReplicateCommandContext context) {
		log.log(Level.FINE, "Creating key comparator with TTL tolerance of {0} seconds",
				replicateOptions.getTtlTolerance());
		RedisItemReader<String, String, KeyComparison> reader = configure(context.comparator()).build();
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		SimpleStepBuilder<KeyComparison, KeyComparison> step = step(context, "verification", reader, null, writer);
		if (replicateOptions.isShowDiffs()) {
			step.listener(new KeyComparisonWriteListener(new KeyComparisonLogger(log)));
		}
		step.listener(new KeyComparisonStepListener(writer, getTransferOptions().getProgressUpdateInterval()));
		ProgressMonitor monitor = progressMonitor().task("Verifying").initialMax(estimator(context))
				.extraMessage(() -> extraMessage(writer.getResults())).build();
		return step(step, monitor).build();
	}

	private ComparatorBuilder configure(ComparatorBuilder builder) {
		return builder.rightPoolOptions(readerOptions.poolOptions()).scanOptions(readerOptions.scanOptions())
				.ttlTolerance(replicateOptions.getTtlToleranceDuration());
	}

	private String extraMessage(Results results) {
		return String.format(extraMessageFormat(), results.getCount(Status.MISSING), results.getCount(Status.TYPE),
				results.getCount(Status.VALUE), results.getCount(Status.TTL));
	}

	private String extraMessageFormat() {
		ProgressBarStyle progressStyle = getTransferOptions().getProgressBarStyle();
		switch (progressStyle) {
		case COLORFUL_UNICODE_BAR:
		case COLORFUL_UNICODE_BLOCK:
			return COMPARE_MESSAGE_COLOR;
		default:
			return COMPARE_MESSAGE_ASCII;
		}
	}

	public ReplicateOptions getReplicateOptions() {
		return replicateOptions;
	}

	public void setReplicationOptions(ReplicateOptions replicationOptions) {
		this.replicateOptions = replicationOptions;
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
		switch (replicateOptions.getMode()) {
		case COMPARE:
			return compareJob(context);
		case LIVE:
			return liveJob(context);
		case LIVEONLY:
			return liveOnlyJob(context);
		case SNAPSHOT:
			return snapshotJob(context);
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicateOptions.getMode());
		}
	}

	private Job liveOnlyJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = context.getJobRunner().job("liveonly-replication").start(liveStep(context));
		return job.build();
	}

	private Job compareJob(ReplicateCommandContext context) {
		SimpleJobBuilder job = context.getJobRunner().job("compare").start(verificationStep(context));
		return job.build();
	}

	private Job snapshotJob(ReplicateCommandContext context) {
		SimpleJobBuilder snapshotJob = context.getJobRunner().job("snapshot-replication").start(scanStep(context));
		optionalVerificationStep(context).ifPresent(snapshotJob::next);
		return snapshotJob.build();
	}

	private Job liveJob(ReplicateCommandContext context) {
		TaskletStep liveStep = liveStep(context);
		SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-flow").start(liveStep).build();
		TaskletStep scanStep = scanStep(context);
		SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-flow").start(scanStep).build();
		SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow")
				.split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow).build();
		JobFlowBuilder liveJob = context.getJobRunner().job("live-replication").start(replicationFlow);
		optionalVerificationStep(context).ifPresent(liveJob::next);
		return liveJob.build().build();
	}

	protected Optional<Step> optionalVerificationStep(ReplicateCommandContext context) {
		if (replicateOptions.isNoVerify()) {
			return Optional.empty();
		}
		if (writerOptions.isDryRun()) {
			return Optional.empty();
		}
		if (replicateOptions.getKeyProcessor().isPresent()) {
			// Verification cannot be done if a processor is set
			log.warning("Key processor enabled, verification will be skipped");
			return Optional.empty();
		}
		return Optional.of(verificationStep(context));
	}

	private TaskletStep scanStep(ReplicateCommandContext context) {
		RedisItemReader reader = reader(context).options(readerOptions.readerOptions())
				.scanOptions(readerOptions.scanOptions()).build();
		RedisItemWriter writer = checkWriter(context).build();
		ScanSizeEstimator estimator = estimator(context);
		ProgressMonitor monitor = progressMonitor().task("Scanning").initialMax(estimator).build();
		return step(step(context, "snapshot-replication", reader, processor(context), writer), monitor).build();
	}

	private TaskletStep liveStep(ReplicateCommandContext context) {
		RedisItemReader reader = liveReader(context).build();
		RedisItemWriter writer = checkWriter(context).build();
		StepOptions stepOptions = liveStepOptions(stepOptions());
		ItemProcessor<byte[], ?> processor = processor(context);
		SimpleStepBuilder<byte[], ?> step = step(context, "live-replication", reader, processor, writer, stepOptions);
		ProgressMonitor monitor = progressMonitor().task("Listening").build();
		return step(step, monitor).build();
	}

	private LiveBuilder<byte[], byte[], ?> liveReader(ReplicateCommandContext context) {
		LiveBuilder<byte[], byte[], ?> builder = reader(context).scanOptions(readerOptions.scanOptions()).live();
		ReaderOptions liveReaderOptions = readerOptions.readerOptions();
		liveStepOptions(liveReaderOptions.getStepOptions());
		builder.options(liveReaderOptions);
		builder.eventQueueOptions(replicateOptions.notificationQueueOptions());
		builder.database(context.getRedisURI().getDatabase());
		replicateOptions.getKeySlot().map(this::keySlotFilter).ifPresent(builder::keyFilter);
		return builder;
	}

	private StepOptions liveStepOptions(StepOptions stepOptions) {
		stepOptions.setFlushingInterval(Duration.ofMillis(replicateOptions.getFlushInterval()));
		if (replicateOptions.getIdleTimeout() > 0) {
			stepOptions.setIdleTimeout(Duration.ofMillis(replicateOptions.getIdleTimeout()));
		}
		return stepOptions;
	}

	private AbstractBuilder<byte[], byte[], ?, ?> checkWriter(ReplicateCommandContext context) {
		if (writerOptions.isDryRun()) {
			return RedisItemWriter.operation(context.getTargetRedisClient(), ByteArrayCodec.INSTANCE, NOOP_OPERATION);
		}
		if (isDataStructure()) {
			return configure(context.targetDataStructureWriter(ByteArrayCodec.INSTANCE)
					.dataStructureOptions(writerOptions.dataStructureOptions()));
		}
		return configure(context.targetKeyDumpWriter());
	}

	private ScanBuilder<byte[], byte[], ?> reader(ReplicateCommandContext context) {
		if (isDataStructure()) {
			return context.dataStructureReader(ByteArrayCodec.INSTANCE);
		}
		return context.keyDumpReader();
	}

	private Predicate<byte[]> keySlotFilter(IntRange range) {
		return SlotRangeFilter.of(ByteArrayCodec.INSTANCE, range.getMin(), range.getMax());
	}

	private <B extends AbstractBuilder> B configure(B writer) {
		return (B) writer.options(writerOptions.writerOptions());
	}

	private boolean isDataStructure() {
		return replicateOptions.getStrategy() == ReplicateStrategy.DS;
	}

	private ItemProcessor<byte[], ?> processor(ReplicateCommandContext context) {
		SpelExpressionParser parser = new SpelExpressionParser();
		List<ItemProcessor> processors = new ArrayList<>();
		replicateOptions.getKeyProcessor().ifPresent(p -> {
			EvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("src", context.getRedisURI());
			evaluationContext.setVariable("dest", context.getTargetRedisURI());
			Expression expression = parser.parseExpression(p);
			processors.add(new KeyValueProcessor<>(expression, evaluationContext));
		});
		return CompositeItemStreamItemProcessor.delegates(processors.toArray(new ItemProcessor[0]));
	}

}
