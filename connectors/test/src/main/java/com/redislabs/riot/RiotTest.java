package com.redislabs.riot;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import io.lettuce.core.RedisURI;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.Assertions;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import picocli.CommandLine;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.function.Consumer;

public abstract class RiotTest {

    protected abstract RiotApp app();

    protected String[] args(String filename) throws Exception {
        try (InputStream inputStream = getClass().getResourceAsStream("/" + filename)) {
            String command = IOUtils.toString(inputStream, Charset.defaultCharset());
            if (command.startsWith("riot-")) {
                command = command.substring(command.indexOf(" ") + 1);
            }
            return CommandLineUtils.translateCommandline(command);
        }
    }

    protected int execute(String filename, RedisContainer container, Consumer<CommandLine.ParseResult>... configurators) throws Exception {
        RiotApp app = app();
        RiotCommandLine commandLine = app.commandLine();
        CommandLine.ParseResult parseResult = commandLine.parseArgs(args(filename));
        configure(app, container);
        for (Consumer<CommandLine.ParseResult> configurator : configurators) {
            configurator.accept(parseResult);
        }
        return commandLine.getExecutionStrategy().execute(parseResult);
    }

    private void configure(RiotApp app, RedisContainer container) {
        app.setInfo(true);
        app.getRedisOptions().setUris(new RedisURI[]{RedisURI.create(container.getRedisURI())});
        app.getRedisOptions().setCluster(container instanceof RedisClusterContainer);
    }

    protected void awaitTermination(JobExecution execution) throws InterruptedException {
        while (execution.isRunning()) {
            Thread.sleep(10);
        }
        for (StepExecution stepExecution : execution.getStepExecutions()) {
            Assertions.assertEquals(ExitStatus.COMPLETED.getExitCode(), stepExecution.getExitStatus().getExitCode());
        }
    }
}
