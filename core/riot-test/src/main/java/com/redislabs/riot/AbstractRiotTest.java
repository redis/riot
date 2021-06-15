package com.redislabs.riot;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.RedisURI;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import picocli.CommandLine;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public abstract class AbstractRiotTest {

    protected abstract RiotApp app();

    @SuppressWarnings("ConstantConditions")
    protected String[] args(String filename) throws Exception {
        try (InputStream inputStream = getClass().getResourceAsStream("/" + filename)) {
            String command = IOUtils.toString(inputStream, Charset.defaultCharset());
            if (command.startsWith("riot-")) {
                command = command.substring(command.indexOf(" ") + 1);
            }
            return CommandLineUtils.translateCommandline(command);
        }
    }

    protected int execute(String filename, RedisServer container, Consumer<CommandLine.ParseResult>... configurators) throws Exception {
        RiotApp app = app();
        RiotCommandLine commandLine = app.commandLine();
        CommandLine.ParseResult parseResult = commandLine.parseArgs(args(filename));
        configure(app, container);
        for (Consumer<CommandLine.ParseResult> configurator : configurators) {
            configurator.accept(parseResult);
        }
        return commandLine.getExecutionStrategy().execute(parseResult);
    }

    protected void configure(RiotApp app, RedisServer container) {
        app.setInfo(true);
        app.getRedisOptions().setUris(new RedisURI[]{RedisURI.create(container.getRedisURI())});
        app.getRedisOptions().setCluster(container instanceof RedisClusterContainer);
    }

}
