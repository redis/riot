package com.redis.riot;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

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
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.Openable;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine.ParseResult;

@TestInstance(Lifecycle.PER_CLASS)
@SuppressWarnings("unchecked")
public abstract class AbstractTestBase {

	private static final String PREFIX = "riot ";
	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(1);
	private static final GeneratorReaderOptions DEFAULT_GENERATOR_OPTIONS = GeneratorReaderOptions.builder().build();
	protected static final int DEFAULT_BATCH_SIZE = 50;
	private static final int DEFAULT_GENERATOR_COUNT = 100;
	private static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	private static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

	private JobRunner jobRunner;

	protected AbstractRedisClient client;
	protected StatefulRedisModulesConnection<String, String> connection;

	protected abstract RedisServer getRedisServer();

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	@BeforeAll
	void setupRedis() {
		RedisServer redis = getRedisServer();
		redis.start();
		client = client(redis);
		connection = RedisModulesUtils.connection(client);
	}

	@BeforeAll
	void setupJobRunner() {
		jobRunner = JobRunner.inMemory().runningTimeout(DEFAULT_RUNNING_TIMEOUT)
				.terminationTimeout(DEFAULT_TERMINATION_TIMEOUT);
	}

	@AfterAll
	void teardown() {
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
		getRedisServer().close();
	}

	@BeforeEach
	void flushAll() {
		connection.sync().flushall();
	}

	protected void awaitOpen(Object object) {
		if (object instanceof Openable) {
			awaitUntil(((Openable) object)::isOpen);
		}
	}

	protected void awaitClosed(Object object) {
		if (object instanceof Openable) {
			awaitUntilFalse(((Openable) object)::isOpen);
		}
	}

	protected void awaitUntilFalse(Callable<Boolean> conditionEvaluator) {
		awaitUntil(() -> !conditionEvaluator.call());
	}

	protected void awaitUntil(Callable<Boolean> conditionEvaluator) {
		Awaitility.await().timeout(DEFAULT_AWAIT_TIMEOUT).until(conditionEvaluator);
	}

	protected AbstractRedisClient client(RedisServer server) {
		return ClientBuilder.create(RedisURI.create(server.getRedisURI())).cluster(server.isCluster()).build();
	}

	protected RedisItemReader.Builder<String, String> reader(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.client((RedisModulesClusterClient) client);
		}
		return RedisItemReader.client((RedisModulesClient) client);
	}

	protected RedisItemWriter.Builder<String, String> writer(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.client((RedisModulesClusterClient) client);
		}
		return RedisItemWriter.client((RedisModulesClient) client);
	}

	protected int execute(String filename, Consumer<ParseResult>... configurers) throws Exception {
		RedisServer redis = getRedisServer();
		Main app = new Main();
		RiotCommandLine commandLine = app.commandLine();
		ParseResult parseResult = parse(commandLine, filename);
		for (Consumer<ParseResult> configurer : configurers) {
			configurer.accept(parseResult);
		}
		app.getLoggingOptions().setInfo(true);
		app.getLoggingOptions().setStacktrace(true);
		app.getRedisOptions().setPort(0);
		app.getRedisOptions().setHost(Optional.empty());
		app.getRedisOptions().setUri(RedisURI.create(redis.getRedisURI()));
		app.getRedisOptions().setCluster(redis.isCluster());
		return commandLine.getExecutionStrategy().execute(parseResult);
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

	protected ParseResult parse(RiotCommandLine commandLine, String filename) throws Exception {
		ParseResult parseResult = commandLine.parseArgs(args(filename));
		Object command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		if (command instanceof OperationCommand) {
			command = parseResult.subcommand().commandSpec().parent().commandLine().getCommand();
		}
		if (command instanceof AbstractTransferCommand) {
			AbstractTransferCommand transferCommand = (AbstractTransferCommand) command;
			transferCommand.getTransferOptions().setProgressStyle(ProgressStyle.NONE);
		}
		return parseResult;
	}

	protected void generate() throws JobExecutionException {
		generate(DEFAULT_GENERATOR_OPTIONS, DEFAULT_GENERATOR_COUNT);
	}

	protected void generate(int max) throws JobExecutionException {
		generate(DEFAULT_GENERATOR_OPTIONS, max);
	}

	protected void generate(GeneratorReaderOptions options) throws JobExecutionException {
		generate(options, DEFAULT_GENERATOR_COUNT);
	}

	protected void generate(GeneratorReaderOptions options, int max) throws JobExecutionException {
		generate(DEFAULT_BATCH_SIZE, options, max);
	}

	protected void generate(int chunkSize, GeneratorReaderOptions options, int max) throws JobExecutionException {
		GeneratorItemReader reader = new GeneratorItemReader(options);
		reader.setMaxItemCount(max);
		run("generate-" + id(), chunkSize, reader, writer(client).dataStructure());
	}

	protected <T> JobExecution run(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		SimpleStepBuilder<T, T> step = step(name, chunkSize, reader, writer);
		Job job = jobRunner.job(name).start(step.build()).build();
		JobExecution execution = jobRunner.getJobLauncher().run(job, new JobParameters());
		awaitClosed(reader);
		awaitClosed(writer);
		return execution;
	}

	protected String id() {
		return UUID.randomUUID().toString();
	}

	protected <T> SimpleStepBuilder<T, T> step(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer) {
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name + "-reader");
		}
		SimpleStepBuilder<T, T> step = jobRunner.step(name).chunk(chunkSize);
		step.reader(reader).writer(writer);
		return step;
	}

	protected void awaitClosed(RedisItemReader<?, ?> reader) {
		Awaitility.await().until(() -> !reader.isOpen());
	}

}
