package com.redis.riot.cli;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.WriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

@TestInstance(Lifecycle.PER_CLASS)
@SuppressWarnings("unchecked")
public abstract class AbstractTests {

	private static final String PREFIX = "riot ";
	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(1);
	private static final IntRange DEFAULT_GENERATOR_KEY_RANGE = IntRange.to(10000);
	protected static final int DEFAULT_BATCH_SIZE = 50;
	private static final int DEFAULT_GENERATOR_COUNT = 100;

	protected JobRepository jobRepository;
	private JobBuilderFactory jobBuilderFactory;
	private SimpleJobLauncher jobLauncher;
	private StepBuilderFactory stepBuilderFactory;

	protected AbstractRedisClient client;
	protected StatefulRedisModulesConnection<String, String> connection;

	protected abstract RedisServer getRedisServer();

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	@BeforeAll
	void setup() {
		RedisServer redis = getRedisServer();
		redis.start();
		client = client(redis);
		connection = RedisModulesUtils.connection(client);
	}

	@AfterAll
	void teardown() {
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
		getRedisServer().close();
	}

	@BeforeEach
	void setupTest() throws Exception {
		jobRepository = Utils.inMemoryJobRepository();
		jobBuilderFactory = new JobBuilderFactory(jobRepository);
		jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		stepBuilderFactory = new StepBuilderFactory(jobRepository, new ResourcelessTransactionManager());
		connection.sync().flushall();
		RedisModulesCommands<String, String> sync = connection.sync();
		awaitEquals(() -> 0l, sync::dbsize);
	}

	protected void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
		awaitUntil(() -> !conditionEvaluator.call());
	}

	protected void awaitUntil(Callable<Boolean> conditionEvaluator) {
		Awaitility.await().timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
	}

	protected <T> void awaitEquals(Supplier<T> expected, Supplier<T> actual) {
		awaitUntil(() -> expected.get().equals(actual.get()));
	}

	protected AbstractRedisClient client(RedisServer server) {
		return ClientBuilder.create(RedisURI.create(server.getRedisURI())).cluster(server.isCluster()).build();
	}

	protected RedisItemReader<String, String, DataStructure<String>> reader(AbstractRedisClient client) {
		return new ScanBuilder(client).dataStructure(StringCodec.UTF8);
	}

	protected RedisItemWriter<String, String, DataStructure<String>> writer(AbstractRedisClient client) {
		return new WriterBuilder(client).dataStructure();
	}

	protected int execute(String filename, Consumer<ParseResult>... configurers) throws Exception {
		RedisServer redis = getRedisServer();
		CommandLine commandLine = Main.commandLine();
		ParseResult parseResult = commandLine.parseArgs(args(filename));
		configure(parseResult);
		for (Consumer<ParseResult> configurer : configurers) {
			configurer.accept(parseResult);
		}
		Main app = commandLine.getCommand();
		app.getLoggingOptions().setInfo(true);
		app.getLoggingOptions().setStacktrace(true);
		app.getRedisOptions().setPort(0);
		app.getRedisOptions().setHost(Optional.empty());
		app.getRedisOptions().setUri(RedisURI.create(redis.getRedisURI()));
		app.getRedisOptions().setCluster(redis.isCluster());
		return commandLine.getExecutionStrategy().execute(parseResult);
	}

	protected void configure(ParseResult parseResult) {
		for (ParseResult sub : parseResult.subcommands()) {
			Object command = sub.commandSpec().commandLine().getCommand();
			if (command instanceof OperationCommand) {
				command = sub.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractCommand) {
				AbstractCommand transferCommand = (AbstractCommand) command;
				transferCommand.getJobOptions().setProgressUpdateInterval(0);
			}
		}
	}

	private static String[] args(String filename) throws Exception {
		try (InputStream inputStream = Main.class.getResourceAsStream("/" + filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			if (command.startsWith(PREFIX)) {
				command = command.substring(PREFIX.length());
			}
			return CommandLineUtils.translateCommandline(command);
		}
	}

	protected GeneratorItemReader generator() {
		return generator(DEFAULT_GENERATOR_COUNT);
	}

	protected GeneratorItemReader generator(int count) {
		GeneratorItemReader generator = new GeneratorItemReader();
		generator.setKeyRange(DEFAULT_GENERATOR_KEY_RANGE);
		generator.setMaxItemCount(count);
		return generator;
	}

	protected void generate(String name) throws JobExecutionException {
		generate(name, DEFAULT_BATCH_SIZE, generator());
	}

	protected void generate(String name, GeneratorItemReader reader) throws JobExecutionException {
		generate(name, DEFAULT_BATCH_SIZE, reader);
	}

	protected void generate(String name, int chunkSize, GeneratorItemReader reader) throws JobExecutionException {
		run(name + "-generate", chunkSize, reader, writer(client));
	}

	protected <T> JobExecution run(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		SimpleStepBuilder<T, T> step = step(name, chunkSize, reader, writer);
		Job job = jobBuilderFactory.get(name).start(step.build()).build();
		return jobLauncher.run(job, new JobParameters());
	}

	protected String id() {
		return UUID.randomUUID().toString();
	}

	protected <T> SimpleStepBuilder<T, T> step(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer) {
		SimpleStepBuilder<T, T> step = stepBuilderFactory.get(name).chunk(chunkSize);
		step.reader(reader);
		step.writer(writer);
		return step;
	}

}
