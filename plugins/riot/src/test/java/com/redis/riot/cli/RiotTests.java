package com.redis.riot.cli;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.Assertions;

import com.redis.riot.cli.ProgressArgs.ProgressStyle;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.riot.core.ReplicationMode;
import com.redis.spring.batch.test.AbstractTargetTestBase;

import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

public abstract class RiotTests extends AbstractTargetTestBase {

    private static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 100000;

    private static final String PREFIX = "riot ";

    protected static void assertExecutionSuccessful(int exitCode) {
        Assertions.assertEquals(0, exitCode);
    }

    protected <T> T command(ParseResult parseResult) {
        return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
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
        if (command instanceof ReplicateCommand) {
            ReplicateCommand replicationCommand = (ReplicateCommand) command;
            replicationCommand.targetRedisClientArgs.uriArgs.uri = getTargetRedisServer().getRedisURI();
            if (replicationCommand.mode == ReplicationMode.LIVE || replicationCommand.mode == ReplicationMode.LIVEONLY) {
                replicationCommand.readerArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT.toMillis());
                replicationCommand.readerArgs.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
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

}
