package com.redis.riot;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.awaitility.Awaitility;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.opentest4j.AssertionFailedError;
import org.springframework.batch.core.JobExecution;

import com.redis.spring.batch.generator.Generator;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;

import io.lettuce.core.RedisURI;
import picocli.CommandLine;

@SuppressWarnings("unchecked")
public abstract class AbstractRiotTests extends AbstractTestcontainersRedisTestBase {

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

	protected int execute(String filename, RedisTestContext redis, Consumer<CommandLine.ParseResult>... configurators)
			throws Exception {
		RiotApp app = app();
		RiotCommandLine commandLine = app.commandLine();
		CommandLine.ParseResult parseResult = commandLine.parseArgs(args(filename));
		configure(app, redis);
		for (Consumer<CommandLine.ParseResult> configurator : configurators) {
			configurator.accept(parseResult);
		}
		int result = commandLine.getExecutionStrategy().execute(parseResult);
		if (result != 0) {
			throw new AssertionFailedError(filename + " execution failed", 0, result);
		}
		return result;
	}

	protected void configure(RiotApp app, RedisTestContext redis) {
		app.getLoggingOptions().setDebug(true);
		app.getLoggingOptions().setStacktrace(true);
		app.getRedisOptions().setUris(new RedisURI[] { RedisURI.create(redis.getRedisURI()) });
		app.getRedisOptions().setCluster(redis.isCluster());
	}

	protected void execute(Generator.Builder generator) throws Exception {
		awaitTermination(generator.build().call());
	}

	protected void awaitTermination(JobExecution execution) {
		Awaitility.await().timeout(Duration.ofSeconds(60)).until(() -> !execution.isRunning());
	}

}
