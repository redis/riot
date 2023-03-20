package com.redis.riot.replicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
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

import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.KeyValueProcessor;
import com.redis.riot.replicate.ReplicationOptions.ReplicationStrategy;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.SlotRangeFilter;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;
import com.redis.spring.batch.writer.operation.Noop;
import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Command(name = "replicate", description = "Replicate a Redis DB into another Redis DB")
public class ReplicateCommand extends AbstractTargetCommand {

	private static final Logger log = Logger.getLogger(ReplicateCommand.class.getName());

	private static final Noop<byte[], byte[], KeyValue<byte[]>> NOOP_OPERATION = new Noop<>();

	@Mixin
	private FlushingTransferOptions flushingOptions = new FlushingTransferOptions();

	@Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public FlushingTransferOptions getFlushingOptions() {
		return flushingOptions;
	}

	public void setFlushingOptions(FlushingTransferOptions flushingTransferOptions) {
		this.flushingOptions = flushingTransferOptions;
	}

	public ReplicationOptions getReplicationOptions() {
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
	protected Job job(JobCommandContext jobCommandContext) {
		TargetCommandContext context = (TargetCommandContext) jobCommandContext;
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-flow").start(liveStep(context)).build();
			SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-flow").start(scanStep(context)).build();
			SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow")
					.split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow).build();
			JobFlowBuilder liveJob = context.job("live-replication").start(replicationFlow);
			optionalVerificationStep(context).ifPresent(liveJob::next);
			return liveJob.build().build();
		case LIVEONLY:
			return job(context, "liveonly-replication", this::liveStep);
		case SNAPSHOT:
			return job(context, "snapshot-replication", this::scanStep);
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicationOptions.getMode());
		}
	}

	private Job job(TargetCommandContext context, String name, Function<TargetCommandContext, Step> step) {
		SimpleJobBuilder job = context.job(name).start(step.apply(context));
		optionalVerificationStep(context).ifPresent(job::next);
		return job.build();
	}

	protected Optional<Step> optionalVerificationStep(TargetCommandContext context) {
		if (replicationOptions.isVerify()) {
			if (writerOptions.isDryRun()) {
				return Optional.empty();
			}
			if (replicationOptions.getKeyProcessor().isPresent()) {
				// Verification cannot be done if a processor is set
				log.warning("Key processor enabled, verification will be skipped");
				return Optional.empty();
			}
			return Optional.of(verificationStep(context));
		}
		return Optional.empty();
	}

	private TaskletStep scanStep(TargetCommandContext context) {
		RedisItemReader reader = scanReader(context);
		reader.setName("scan-reader");
		RedisItemWriter writer = checkWriter(context);
		ScanSizeEstimator estimator = estimator(context);
		ProgressMonitor monitor = progressMonitor().task("Scanning").initialMax(estimator::execute).build();
		return step(step(context, "snapshot-replication", reader, processor(context), writer), monitor).build();
	}

	private RedisItemReader<byte[], ?> scanReader(TargetCommandContext context) {
		Builder<byte[], byte[]> builder = reader(context);
		if (isTypeBasedReplication()) {
			return builder.dataStructure();
		}
		return builder.keyDump();
	}

	private TaskletStep liveStep(TargetCommandContext context) {
		LiveRedisItemReader reader = liveReader(context);
		reader.setName("live-reader");
		RedisItemWriter writer = checkWriter(context);
		SimpleStepBuilder<byte[], ?> step = step(context, "live-replication", reader, processor(context), writer);
		FlushingSimpleStepBuilder liveStep = JobRunner.flushing(step, flushingOptions.flushingOptions());
		ProgressMonitor monitor = progressMonitor().task("Listening").build();
		return step(liveStep, monitor).build();
	}

	private RedisItemWriter<byte[], byte[], ?> checkWriter(TargetCommandContext context) {
		if (writerOptions.isDryRun()) {
			return context.targetWriter(ByteArrayCodec.INSTANCE).operation(NOOP_OPERATION);
		}
		return writer(context);
	}

	private LiveRedisItemReader<byte[], ?> liveReader(JobCommandContext context) {
		LiveRedisItemReader.Builder<byte[], byte[]> builder = reader(context).live();
		builder.flushingOptions(flushingOptions.flushingOptions());
		builder.queueOptions(replicationOptions.notificationQueueOptions());
		builder.database(context.getRedisURI().getDatabase());
		replicationOptions.getKeySlot().map(this::keySlotFilter).ifPresent(builder::keyFilter);
		if (isTypeBasedReplication()) {
			return builder.dataStructure();
		}
		return builder.keyDump();
	}

	private Predicate<byte[]> keySlotFilter(IntRange range) {
		return SlotRangeFilter.of(ByteArrayCodec.INSTANCE, range.getMin(), range.getMax());
	}

	private Builder<byte[], byte[]> reader(JobCommandContext context) {
		Builder<byte[], byte[]> builder = context.reader(ByteArrayCodec.INSTANCE);
		builder.readerOptions(readerOptions.readerOptions());
		builder.scanOptions(readerOptions.scanOptions());
		return builder;
	}

	private RedisItemWriter<byte[], byte[], ?> writer(TargetCommandContext context) {
		RedisItemWriter.Builder<byte[], byte[]> builder = context.targetWriter(ByteArrayCodec.INSTANCE);
		builder.options(writerOptions.writerOptions());
		if (isTypeBasedReplication()) {
			return builder.dataStructure(Xadd.identity());
		}
		return builder.keyDump();
	}

	private boolean isTypeBasedReplication() {
		return replicationOptions.getStrategy() == ReplicationStrategy.DS;
	}

	private ItemProcessor<byte[], ?> processor(TargetCommandContext context) {
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
