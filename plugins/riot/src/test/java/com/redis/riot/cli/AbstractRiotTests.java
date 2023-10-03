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
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.ProgressArgs.ProgressStyle;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.writer.StructItemWriter;
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

    private static final Range DEFAULT_GENERATOR_KEY_RANGE = Range.to(10000);

    protected static final int DEFAULT_BATCH_SIZE = 50;

    private static final int DEFAULT_GENERATOR_COUNT = 100;

    protected JobRepository jobRepository;

    private JobBuilderFactory jobBuilderFactory;

    private SimpleJobLauncher jobLauncher;

    private StepBuilderFactory stepBuilderFactory;

    protected AbstractRedisClient client;

    protected StatefulRedisModulesConnection<String, String> connection;

    private PlatformTransactionManager transactionManager;

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
        transactionManager = bean.getTransactionManager();
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
        if (server.isCluster()) {
            return RedisModulesClusterClient.create(uri);
        }
        return RedisModulesClient.create(uri);
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
        generator.setTypes(DataType.ZSET);
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
        awaitUntilFalse(reader::isOpen);
    }

    protected StructItemWriter<String, String> structWriter(AbstractRedisClient client) {
        return new StructItemWriter<>(client, StringCodec.UTF8);
    }

    protected StructItemReader<String, String> structReader(AbstractRedisClient client) {
        StructItemReader<String, String> reader = new StructItemReader<>(client, StringCodec.UTF8);
        reader.setJobRepository(jobRepository);
        reader.setTransactionManager(transactionManager);
        return reader;
    }

    protected <T> JobExecution run(String name, int chunkSize, ItemReader<T> reader, ItemWriter<T> writer)
            throws JobExecutionException {
        SimpleStepBuilder<T, T> step = step(name, chunkSize, reader, writer);
        return jobLauncher.run(jobBuilderFactory.get(name).start(step.build()).build(), new JobParameters());
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
