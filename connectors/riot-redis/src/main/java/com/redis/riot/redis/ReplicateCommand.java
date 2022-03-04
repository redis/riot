package com.redis.riot.redis;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.KeyValueProcessorOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStep;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.reader.ScanRedisItemReaderBuilder;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;

@CommandLine.Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB")
public class ReplicateCommand extends AbstractTargetCommand {

	private static final Logger log = LoggerFactory.getLogger(ReplicateCommand.class);

	@CommandLine.Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@CommandLine.Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();
	@CommandLine.Mixin
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
	protected Job job(JobBuilder jobBuilder) throws Exception {
		Optional<Step> verificationStep = optionalVerificationStep();
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow liveFlow = new FlowBuilder<SimpleFlow>("live-replication-flow").start(liveReplicationStep())
					.build();
			SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-replication-flow").start(scanStep()).build();
			SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow")
					.split(new SimpleAsyncTaskExecutor()).add(liveFlow, scanFlow).build();
			JobFlowBuilder jobFlowBuilder = jobBuilder.start(replicationFlow);
			verificationStep.ifPresent(jobFlowBuilder::next);
			return jobFlowBuilder.build().build();
		case LIVEONLY:
			SimpleJobBuilder liveReplicationJob = jobBuilder.start(liveReplicationStep());
			verificationStep.ifPresent(liveReplicationJob::next);
			return liveReplicationJob.build();
		case SNAPSHOT:
			SimpleJobBuilder scanReplicationJob = jobBuilder.start(scanStep());
			verificationStep.ifPresent(scanReplicationJob::next);
			return scanReplicationJob.build();
		default:
			throw new IllegalArgumentException("Unknown replication mode: " + replicationOptions.getMode());
		}
	}

	protected Optional<Step> optionalVerificationStep() throws Exception {
		if (replicationOptions.isVerify()) {
			if (replicationOptions.isDryRun()) {
				return Optional.empty();
			}
			if (processorOptions.getKeyProcessor() != null) {
				// Verification cannot be done if a processor is set
				log.warn("Key processor enabled, verification will be skipped");
				return Optional.empty();
			}
			return Optional.of(verificationStep());
		}
		return Optional.empty();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep scanStep() throws Exception {
		RiotStep.Builder riotStep = RiotStep.builder().name("scan-replication-step").taskName("Scanning");
		RedisItemReader<byte[], ?> reader = reader().build();
		reader.setName("redis-scan-reader");
		riotStep.reader(reader);
		initialMax(riotStep);
		return step(configure(riotStep).build()).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep liveReplicationStep() throws Exception {
		RedisItemReader<byte[], ?> reader = flushingTransferOptions
				.configure(reader().live().keyPatterns(readerOptions.getScanMatch())
						.notificationQueueCapacity(replicationOptions.getNotificationQueueCapacity())
						.database(getRedisOptions().uris().get(0).getDatabase()))
				.build();
		reader.setName("redis-live-reader");
		RiotStep.Builder liveStep = RiotStep.builder().name("live-replication-step").taskName("Listening");
		return flushingTransferOptions.configure(step(configure(liveStep.reader(reader)).build())).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RiotStep.Builder configure(RiotStep.Builder step) {
		return step.processor(processorOptions.processor(getRedisOptions(), targetRedisOptions)).writer(writer());
	}

	private ItemWriter<?> writer() {
		if (replicationOptions.isDryRun()) {
			log.debug("Using no-op writer");
			return new NoOpItemWriter<>();
		}
		log.debug("Configuring writer with {}", targetRedisOptions);
		OperationBuilder<byte[], byte[]> builder = writer(targetRedisOptions, ByteArrayCodec.INSTANCE);
		switch (replicationOptions.getType()) {
		case DS:
			return writerOptions.configureWriter(builder.dataStructure()).build();
		case DUMP:
			return writerOptions.configureWriter(builder.keyDump()).build();
		default:
			break;
		}
		throw new IllegalArgumentException("Unknown replication type: " + replicationOptions.getType());
	}

	private static class NoOpItemWriter<T> implements ItemWriter<T> {

		@Override
		public void write(List<? extends T> items) throws Exception {
			// Do nothing
		}
	}

	private ScanRedisItemReaderBuilder<byte[], byte[], ?> reader() throws Exception {
		return readerOptions.configureScanReader(
				configureJobRepository(reader(reader(getRedisOptions(), ByteArrayCodec.INSTANCE))));
	}

	private ScanRedisItemReaderBuilder<byte[], byte[], ?> reader(RedisItemReader.Builder<byte[], byte[]> reader) {
		switch (replicationOptions.getType()) {
		case DS:
			return reader.dataStructure();
		case DUMP:
			return reader.keyDump();
		default:
			break;
		}
		throw new IllegalArgumentException("Unknown replication type: " + replicationOptions.getType());
	}

}
