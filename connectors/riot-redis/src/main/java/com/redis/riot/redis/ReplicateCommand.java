package com.redis.riot.redis;

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
import com.redis.riot.RiotStepBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter.BaseRedisItemWriterBuilder;
import com.redis.spring.batch.RedisItemWriter.OperationItemWriterBuilder;
import com.redis.spring.batch.builder.ScanRedisItemReaderBuilder;

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
			SimpleFlow notificationFlow = new FlowBuilder<SimpleFlow>("live-replication-flow")
					.start(liveReplicationStep()).build();
			SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>("scan-replication-flow").start(scanStep()).build();
			SimpleFlow replicationFlow = new FlowBuilder<SimpleFlow>("replication-flow")
					.split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow).build();
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
			if (processorOptions.getKeyProcessor() == null) {
				return Optional.of(verificationStep());
			}
			// Verification cannot be done if a processor is set
			log.warn("Key processor enabled, verification will be skipped");
		}
		return Optional.empty();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep scanStep() throws Exception {
		RedisItemReader reader = reader().build();
		reader.setName("redis-scan-reader");
		RiotStepBuilder scanStep = riotStep("scan-replication-step", "Scanning");
		initialMax(scanStep);
		return configure(scanStep.reader(reader)).build().build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep liveReplicationStep() throws Exception {
		RedisItemReader reader = reader().live().keyPatterns(readerOptions.getScanMatch())
				.notificationQueueCapacity(replicationOptions.getNotificationQueueCapacity())
				.database(getRedisOptions().uris().get(0).getDatabase())
				.flushingInterval(flushingTransferOptions.getFlushIntervalDuration())
				.idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()).build();
		reader.setName("redis-live-reader");
		return configure(
				riotStep("live-replication-step", "Listening").reader(reader).flushingOptions(flushingTransferOptions))
						.build().build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RiotStepBuilder configure(RiotStepBuilder step) {
		return step.processor(processorOptions.processor(getRedisOptions(), targetRedisOptions)).writer(writer());
	}

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() {
		log.debug("Configuring writer with {}", targetRedisOptions);
		OperationItemWriterBuilder<String, String> writer = writer(targetRedisOptions);
		return writerOptions.configureWriter(redisWriter(writer)).build();
	}

	@SuppressWarnings("rawtypes")
	private BaseRedisItemWriterBuilder redisWriter(OperationItemWriterBuilder<String, String> builder) {
		switch (replicationOptions.getType()) {
		case DS:
			return builder.dataStructure();
		case DUMP:
			return builder.keyDump();
		default:
			break;
		}
		throw new IllegalArgumentException("Unknown replication type: " + replicationOptions.getType());
	}

	@SuppressWarnings("rawtypes")
	private ScanRedisItemReaderBuilder reader() throws Exception {
		return readerOptions.configureScanReader(configureJobRepository(reader(reader(getRedisOptions()))));
	}

	@SuppressWarnings("rawtypes")
	private ScanRedisItemReaderBuilder reader(ItemReaderBuilder reader) {
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
