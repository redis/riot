package com.redis.riot.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.KeyValueProcessorOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStep;
import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.KeyValueKeyProcessor;
import com.redis.riot.processor.KeyValueTTLProcessor;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.reader.LiveRedisItemReader.Builder;
import com.redis.spring.batch.writer.operation.Noop;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Mixin;

public abstract class AbstractReplicateCommand<T extends KeyValue<byte[], ?>> extends AbstractTargetCommand {

	private static final Logger log = Logger.getLogger(AbstractReplicateCommand.class.getName());

	@Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();
	@Mixin
	private KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public FlushingTransferOptions getFlushingTransferOptions() {
		return flushingTransferOptions;
	}

	public ReplicationOptions getReplicationOptions() {
		return replicationOptions;
	}

	public KeyValueProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	@Override
	protected Job createJob(TargetCommandContext context) {
		Optional<Step> verificationStep = optionalVerificationStep(context);
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-replication-live-flow")
					.start(liveReplicationStep(context)).build();
			SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("live-replication-scan-flow").start(scanStep(context))
					.build();
			SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow")
					.split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow).build();
			JobFlowBuilder liveJob = context.getJobRunner().job("live-replication").start(replicationFlow);
			verificationStep.ifPresent(liveJob::next);
			return liveJob.build().build();
		case LIVEONLY:
			SimpleJobBuilder liveonlyJob = context.getJobRunner().job("liveonly-replication")
					.start(liveReplicationStep(context));
			verificationStep.ifPresent(liveonlyJob::next);
			return liveonlyJob.build();
		case SNAPSHOT:
			SimpleJobBuilder snapshotJob = context.getJobRunner().job("snapshot-replication").start(scanStep(context));
			verificationStep.ifPresent(snapshotJob::next);
			return snapshotJob.build();
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicationOptions.getMode());
		}
	}

	protected Optional<Step> optionalVerificationStep(TargetCommandContext context) {
		if (replicationOptions.isVerify()) {
			if (writerOptions.isDryRun()) {
				return Optional.empty();
			}
			if (processorOptions.getKeyProcessor().isPresent()) {
				// Verification cannot be done if a processor is set
				log.warning("Key processor enabled, verification will be skipped");
				return Optional.empty();
			}
			return Optional.of(verificationStep(context));
		}
		return Optional.empty();
	}

	private TaskletStep scanStep(TargetCommandContext context) {
		RedisItemReader.Builder<byte[], byte[], T> reader = reader(context, "scan-reader");
		RedisItemWriter<byte[], byte[], T> writer = createWriter(context).build();
		return step(context, RiotStep.reader(reader.build()).writer(writer).processor(processor(context))
				.name("snapshot-replication").taskName("Scanning").max(estimator(context).build()::execute).build())
				.build();
	}

	private TaskletStep liveReplicationStep(TargetCommandContext context) {
		Builder<byte[], byte[], T> reader = reader(context, "redis-live-reader").live()
				.notificationQueueCapacity(replicationOptions.getNotificationQueueCapacity())
				.database(context.getRedisOptions().uri().getDatabase());
		RedisItemWriter<byte[], byte[], T> writer = createWriter(context).build();
		RiotStep<T, T> step = RiotStep.reader(flushingTransferOptions.configure(reader).build()).writer(writer)
				.processor(processor(context)).name("live-replication").taskName("Listening").build();
		return flushingTransferOptions.configure(step(context, step)).build();
	}

	private RedisItemWriter.Builder<byte[], byte[], T> createWriter(TargetCommandContext context) {
		if (writerOptions.isDryRun()) {
			return RedisItemWriter.operation(context.getTargetRedisClient(), ByteArrayCodec.INSTANCE, new Noop<>());
		}
		return writerOptions.configure(writer(context));
	}

	private RedisItemReader.Builder<byte[], byte[], T> reader(TargetCommandContext context, String name) {
		return readerOptions.configure(reader(context)).name(name);
	}

	protected abstract RedisItemWriter.Builder<byte[], byte[], T> writer(TargetCommandContext context);

	protected abstract RedisItemReader.Builder<byte[], byte[], T> reader(TargetCommandContext context);

	private ItemProcessor<T, T> processor(TargetCommandContext context) {
		SpelExpressionParser parser = new SpelExpressionParser();
		List<ItemProcessor<? extends KeyValue<byte[], ?>, ? extends KeyValue<byte[], ?>>> processors = new ArrayList<>();
		processorOptions.getKeyProcessor().ifPresent(p -> {
			EvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("src", context.getRedisOptions().uri());
			evaluationContext.setVariable("dest", context.getTargetRedisOptions().uri());
			Expression expression = parser.parseExpression(p, new TemplateParserContext());
			processors.add(new KeyValueKeyProcessor<>(expression, evaluationContext));
		});
		processorOptions.getTtlProcessor().ifPresent(p -> processors
				.add(new KeyValueTTLProcessor<>(parser.parseExpression(p), new StandardEvaluationContext())));
		return CompositeItemStreamItemProcessor.delegates(processors.toArray(ItemProcessor[]::new));
	}

}
