package com.redislabs.riot.test;

import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redislabs.riot.RiotApp;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

@Testcontainers
@SuppressWarnings("rawtypes")
public abstract class BaseTest {

	private final static String COMMAND_PREAMBLE = "❯";
	protected final static String LOCALHOST = "localhost";
	private static final int REDIS_PORT = 6379;
	private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("redislabs/redisearch:latest");

	protected RedisURI redisURI;
	protected RedisClient client;
	protected StatefulRedisConnection<String, String> connection;
	protected RedisCommands<String, String> sync;

	@Container
	protected static final GenericContainer redis = redisContainer();

	@SuppressWarnings("resource")
	protected static GenericContainer redisContainer() {
		return new GenericContainer(DOCKER_IMAGE_NAME).withExposedPorts(REDIS_PORT);
	}

	@BeforeEach
	public void setup() {
		redisURI = redisURI(redis);
		client = RedisClient.create(redisURI);
		connection = client.connect();
		sync = connection.sync();
		sync.flushall();
	}

	@AfterEach
	public void teardown() {
		connection.close();
		client.shutdown();
	}

	protected String process(String command) {
		return baseArgs() + " " + connectionArgs(redis) + removeConnectionArgs(command);
	}

	private String baseArgs() {
		return "--no-progress --debug";
	}

	private String removeConnectionArgs(String command) {
		return command.replace("-h localhost -p 6379", "");
	}

	protected String connectionArgs(GenericContainer redis) {
		return "-h " + redis.getHost() + " -p " + redis.getFirstMappedPort();
	}

	protected int executeFile(String filename) throws Exception {
		return app().execute(args(filename));
	}

	protected abstract RiotApp app();

	protected String[] args(String filename) throws Exception {
		try (InputStream inputStream = getClass().getResourceAsStream(filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			return CommandLineUtils.translateCommandline(process(removePreamble(command)));
		}
	}

	protected RedisURI redisURI(GenericContainer redis) {
		return RedisURI.create(redis.getHost(), redis.getFirstMappedPort());
	}

	protected String commandPrefix() {
		return COMMAND_PREAMBLE + " " + applicationName();
	}

	protected abstract String applicationName();

	private String removePreamble(String command) {
		if (command.startsWith(commandPrefix())) {
			return command.substring(commandPrefix().length());
		}
		return command;
	}

	protected Object command(String file) throws Exception {
		RiotApp app = app();
		CommandLine commandLine = app.commandLine();
		ParseResult parseResult = app.parse(commandLine, args(file));
		return parseResult.subcommand().commandSpec().commandLine().getCommand();
	}

}
