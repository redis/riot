package com.redis.riot.cli;

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
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.DataStructureWriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.StepOptions;
import com.redis.spring.batch.reader.DataStructureStringReadOperation;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorReaderOptions;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

@TestInstance(Lifecycle.PER_CLASS)
@SuppressWarnings("unchecked")
public abstract class AbstractTestBase {

	private static final String PREFIX = "riot ";
	private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(1);
	private static final GeneratorReaderOptions DEFAULT_GENERATOR_OPTIONS = GeneratorReaderOptions.builder()
			.keyRange(IntRange.to(10000)).build();
	protected static final int DEFAULT_BATCH_SIZE = 50;
	private static final int DEFAULT_GENERATOR_COUNT = 100;
	private static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	private static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

	protected JobRunner jobRunner;

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
		jobRunner = JobRunner.inMemory().runningTimeout(DEFAULT_RUNNING_TIMEOUT)
				.terminationTimeout(DEFAULT_TERMINATION_TIMEOUT);
		connection.sync().flushall();
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

	protected ScanBuilder<String, String, DataStructure<String>> reader(AbstractRedisClient client) {
		return new ScanBuilder<>(client, StringCodec.UTF8, new DataStructureStringReadOperation(client));
	}

	protected DataStructureWriterBuilder<String, String> writer(AbstractRedisClient client) {
		return new DataStructureWriterBuilder<>(client, StringCodec.UTF8);
	}

	protected int execute(String filename, Consumer<ParseResult>... configurers) throws Exception {
		RedisServer redis = getRedisServer();
		Main app = new Main();
		CommandLine commandLine = app.commandLine();
		ParseResult parseResult = commandLine.parseArgs(args(filename));
		configure(parseResult);
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

	protected void configure(ParseResult parseResult) {
		for (ParseResult sub : parseResult.subcommands()) {
			Object command = sub.commandSpec().commandLine().getCommand();
			if (command instanceof OperationCommand) {
				command = sub.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractCommand) {
				AbstractCommand transferCommand = (AbstractCommand) command;
				transferCommand.getTransferOptions().setProgressUpdateInterval(0);
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

	protected void generate(String name) throws JobExecutionException {
		generate(name, DEFAULT_GENERATOR_OPTIONS, DEFAULT_GENERATOR_COUNT);
	}

	protected void generate(String name, int max) throws JobExecutionException {
		generate(name, DEFAULT_GENERATOR_OPTIONS, max);
	}

	protected void generate(String name, GeneratorReaderOptions options) throws JobExecutionException {
		generate(name, options, DEFAULT_GENERATOR_COUNT);
	}

	protected void generate(String name, GeneratorReaderOptions options, int max) throws JobExecutionException {
		generate(name, DEFAULT_BATCH_SIZE, options, max);
	}

	protected void generate(String name, int chunkSize, GeneratorReaderOptions options, int max)
			throws JobExecutionException {
		GeneratorItemReader reader = new GeneratorItemReader(options);
		reader.setMaxItemCount(max);
		run(name + "-generate", chunkSize, reader, writer(client).build());
	}

	protected <T> JobExecution run(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer)
			throws JobExecutionException {
		SimpleStepBuilder<T, T> step = step(name, chunkSize, reader, writer);
		Job job = jobRunner.job(name).start(step.build()).build();
		return jobRunner.run(job);
	}

	protected String id() {
		return UUID.randomUUID().toString();
	}

	protected <T> SimpleStepBuilder<T, T> step(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer) {
		return jobRunner.step(name, reader, null, writer, StepOptions.builder().chunkSize(chunkSize).build());
	}

}
