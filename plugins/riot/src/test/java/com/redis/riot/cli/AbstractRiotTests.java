package com.redis.riot.cli;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.event.Level;
import org.slf4j.impl.SimpleLogger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.ProgressArgs.ProgressStyle;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.LongRange;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

@SuppressWarnings("deprecation")
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractRiotTests {

    private static final String PREFIX = "riot ";

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(1);

    private static final LongRange DEFAULT_GENERATOR_KEY_RANGE = LongRange.to(10000);

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
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, Level.DEBUG.name());
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
        MapJobRepositoryFactoryBean bean = new MapJobRepositoryFactoryBean();
        bean.afterPropertiesSet();
        jobRepository = bean.getObject();
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
        stepBuilderFactory = new StepBuilderFactory(jobRepository, new ResourcelessTransactionManager());
        connection.sync().flushall();
        RedisModulesCommands<String, String> sync = connection.sync();
        awaitEquals(() -> 0l, sync::dbsize);
    }

    protected <T> T command(ParseResult parseResult) {
        return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
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
        RedisURI uri = RedisURI.create(server.getRedisURI());
        return ClientBuilder.create(uri).cluster(server.isCluster()).build();
    }

    protected int execute(String filename, IExecutionStrategy... executionStrategies) throws Exception {
        String[] args = args(filename);
        return Main.run(new PrintWriter(System.out), new PrintWriter(System.err), args, executionStrategy(executionStrategies));
    }

    private IExecutionStrategy executionStrategy(IExecutionStrategy... executionStrategies) {
        CompositeExecutionStrategy strategy = new CompositeExecutionStrategy();
        strategy.addDelegates(this::execute);
        strategy.addDelegates(executionStrategies);
        return strategy;
    }

    private int execute(ParseResult parseResult) {
        Main main = (Main) parseResult.commandSpec().commandLine().getCommand();
        main.redisArgs.uriArgs.uri = getRedisServer().getRedisURI();
        main.redisArgs.cluster = getRedisServer().isCluster();
        for (ParseResult subParseResult : parseResult.subcommands()) {
            Object command = subParseResult.commandSpec().commandLine().getCommand();
            if (command instanceof OperationCommand) {
                command = subParseResult.commandSpec().parent().commandLine().getCommand();
            }
            configureCommand(command);
        }
        return ExitCode.OK;
    }

    protected void configureCommand(Object command) {
        if (command instanceof AbstractJobCommand) {
            AbstractJobCommand jobCommand = ((AbstractJobCommand) command);
            jobCommand.progressArgs.style = ProgressStyle.NONE;
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
        run(name + "-generate", chunkSize, reader, structWriter(client));
    }

    protected RedisItemWriter<String, String> structWriter(AbstractRedisClient client) {
        RedisItemWriter<String, String> writer = new RedisItemWriter<>(client, StringCodec.UTF8);
        writer.setValueType(ValueType.STRUCT);
        return writer;
    }

    protected <T> JobExecution run(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer)
            throws JobExecutionException {
        SimpleStepBuilder<T, T> step = step(name, chunkSize, reader, writer);
        Job job = jobBuilderFactory.get(name).start(step.build()).build();
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        awaitClosed(reader);
        awaitClosed(writer);
        return execution;
    }

    private void awaitClosed(Object object) {
        awaitUntil(() -> BatchUtils.isClosed(object));
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
