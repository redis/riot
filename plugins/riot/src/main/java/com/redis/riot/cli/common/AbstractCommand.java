package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.riot.cli.Main;
import com.redis.riot.core.ThrottledItemWriter;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true)
public abstract class AbstractCommand implements Callable<Integer> {

	private static final int EXIT_VALUE_FAILURE = 1;
	private static final int EXIT_VALUE_SUCCESS = 0;

	@ParentCommand
	private Main riot;

	@Spec
	private CommandSpec commandSpec;

	@Mixin
	private HelpOptions helpOptions;

	@ArgGroup(exclusive = false, heading = "Job options%n")
	private JobOptions jobOptions = new JobOptions();

	private JobRepository jobRepository;
	private JobLauncher jobLauncher;
	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;
	private PlatformTransactionManager transactionManager;

	protected JobRepository getJobRepository() {
		if (jobRepository == null) {
			try {
				jobRepository = Utils.inMemoryJobRepository();
			} catch (Exception e) {
				throw new JobBuilderException(e);
			}
		}
		return jobRepository;
	}

	private JobBuilderFactory jobBuilderFactory() {
		if (jobBuilderFactory == null) {
			jobBuilderFactory = new JobBuilderFactory(getJobRepository());
		}
		return jobBuilderFactory;
	}

	private StepBuilderFactory stepBuilderFactory() {
		if (stepBuilderFactory == null) {
			stepBuilderFactory = new StepBuilderFactory(getJobRepository(), transactionManager());
		}
		return stepBuilderFactory;
	}

	private PlatformTransactionManager transactionManager() {
		if (transactionManager == null) {
			transactionManager = new ResourcelessTransactionManager();
		}
		return transactionManager;
	}

	private String commandName;

	public void setCommandName(String commandName) {
		this.commandName = commandName;
	}

	protected String commandName() {
		if (commandName == null) {
			return commandSpec.qualifiedName("-");
		}
		return commandName;
	}

	public void setRiot(Main riot) {
		this.riot = riot;
	}

	public void setCommandSpec(CommandSpec commandSpec) {
		this.commandSpec = commandSpec;
	}

	protected RedisURI redisURI(RedisOptions redisOptions) {
		RedisURIBuilder builder = RedisURIBuilder.create();
		if (redisOptions.getUri() != null) {
			builder.uri(redisOptions.getUri().toString());
		}
		redisOptions.getHost().ifPresent(builder::host);
		if (redisOptions.getDatabase() > 0) {
			builder.database(redisOptions.getDatabase());
		}
		if (redisOptions.getPort() > 0) {
			builder.port(redisOptions.getPort());
		}
		builder.clientName(redisOptions.getClientName());
		builder.username(redisOptions.getUsername());
		builder.password(redisOptions.getPassword());
		builder.socket(redisOptions.getSocket());
		builder.ssl(redisOptions.isTls());
		builder.sslVerifyMode(redisOptions.getTlsVerifyMode());
		redisOptions.getTimeout().ifPresent(builder::timeoutInSeconds);
		return builder.build();
	}

	protected AbstractRedisClient client(RedisURI redisURI, RedisOptions redisOptions) {
		ClientBuilder builder = ClientBuilder.create(redisURI);
		builder.autoReconnect(!redisOptions.isNoAutoReconnect());
		builder.cluster(redisOptions.isCluster());
		if (redisOptions.isShowMetrics()) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(DefaultEventPublisherOptions.builder()
					.eventEmitInterval(Duration.ofSeconds(redisOptions.getMetricsStep())).build());
		}
		builder.keystore(redisOptions.getKeystore());
		builder.keystorePassword(redisOptions.getKeystorePassword());
		builder.truststore(redisOptions.getTruststore());
		builder.truststorePassword(redisOptions.getTruststorePassword());
		builder.trustManager(redisOptions.getTrustedCerts());
		builder.key(redisOptions.getKey());
		builder.keyCert(redisOptions.getKeyCert());
		builder.keyPassword(redisOptions.getKeyPassword());
		return builder.build();
	}

	@Override
	public Integer call() throws Exception {
		RedisOptions redisOptions = riot.getRedisOptions();
		RedisURI redisURI = redisURI(redisOptions);
		AbstractRedisClient redisClient = client(redisURI, redisOptions);
		try (CommandContext context = context(redisURI, redisClient)) {
			Job job = job(context);
			JobExecution execution = jobLauncher().run(job, new JobParameters());
			if (isFailed(execution)) {
				return EXIT_VALUE_FAILURE;
			}
			return EXIT_VALUE_SUCCESS;
		}

	}

	private boolean isFailed(JobExecution execution) {
		return execution.getStepExecutions().stream().map(StepExecution::getExitStatus).map(ExitStatus::getExitCode)
				.anyMatch(s -> s.equals(ExitStatus.FAILED.getExitCode()));
	}

	private JobLauncher jobLauncher() {
		if (jobLauncher == null) {
			SimpleJobLauncher launcher = new SimpleJobLauncher();
			launcher.setJobRepository(getJobRepository());
			launcher.setTaskExecutor(new SyncTaskExecutor());
			jobLauncher = launcher;
		}
		return jobLauncher;
	}

	protected CommandContext context(RedisURI redisURI, AbstractRedisClient redisClient) {
		return new CommandContext(redisURI, redisClient);
	}

	protected abstract Job job(CommandContext context);

	protected Job job(SimpleStepBuilder<?, ?> step) {
		return job(commandName()).start(step.build()).build();
	}

	protected JobBuilder job(String name) {
		return jobBuilderFactory().get(name);
	}

	public JobOptions getJobOptions() {
		return jobOptions;
	}

	public void setJobOptions(JobOptions options) {
		this.jobOptions = options;
	}

	private <I, O> SimpleStepBuilder<I, O> step(String name) {
		SimpleStepBuilder<I, O> step = stepBuilder(name).chunk(jobOptions.getChunkSize());
		Utils.multiThread(step, jobOptions.getThreads());
		if (jobOptions.getSkipPolicy() == StepSkipPolicy.NEVER) {
			return step;
		}
		return step.faultTolerant().skipPolicy(skipPolicy(jobOptions));
	}

	protected StepBuilder stepBuilder(String name) {
		return stepBuilderFactory().get(name);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
		SimpleStepBuilder<I, O> step = step(name);
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		step.reader(reader);
		step.writer(throttle(writer));
		return step;
	}

	protected StepProgressMonitor monitor(String task) {
		StepProgressMonitor monitor = new StepProgressMonitor();
		monitor.withTask(task);
		monitor.withStyle(jobOptions.getProgressBarStyle());
		monitor.withUpdateInterval(Duration.ofMillis(jobOptions.getProgressUpdateInterval()));
		return monitor;
	}

	private <O> ItemWriter<O> throttle(ItemWriter<O> writer) {
		Duration sleep = Duration.ofMillis(jobOptions.getSleep());
		if (sleep.isNegative() || sleep.isZero()) {
			return writer;
		}
		return new ThrottledItemWriter<>(writer, sleep);
	}

	private SkipPolicy skipPolicy(JobOptions options) {
		switch (options.getSkipPolicy()) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return new LimitCheckingItemSkipPolicy(options.getSkipLimit(), skippableExceptions());
		}
	}

	protected Map<Class<? extends Throwable>, Boolean> skippableExceptions() {
		return Stream
				.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
				.collect(Collectors.toMap(Function.identity(), t -> true));
	}

	protected WriterBuilder writer(AbstractRedisClient client, RedisWriterOptions options) {
		return new WriterBuilder(client).waitReplicas(options.getWaitReplicas())
				.waitTimeout(Duration.ofMillis(options.getWaitTimeout())).multiExec(options.isMultiExec())
				.mergePolicy(options.getMergePolicy()).streamIdPolicy(options.getStreamIdPolicy());
	}

}
