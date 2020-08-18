package com.redislabs.riot.test;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.InputStream;
import java.nio.charset.Charset;

@Testcontainers
@SuppressWarnings("rawtypes")
public abstract class BaseTest {

	private final static String COMMAND_PREAMBLE = "❯";
	protected final static String LOCALHOST = "localhost";
	private static final int REDIS_PORT = 6379;
	private static final String DOCKER_IMAGE_NAME = "redislabs/redisearch:latest";

	private RediSearchClient client;

	@Container
	private static final GenericContainer redis = redisContainer();

	@SuppressWarnings("resource")
	protected static GenericContainer redisContainer() {
		return new GenericContainer(DOCKER_IMAGE_NAME).withExposedPorts(REDIS_PORT);
	}

	@BeforeEach
	public void setup() {
		client = RediSearchClient.create(RedisURI.create(redis.getHost(), redis.getFirstMappedPort()));
		client.connect().sync().flushall();
	}

	protected StatefulRediSearchConnection<String, String> connection() {
		return client.connect();
	}

	protected RediSearchCommands<String, String> commands() {
		return client.connect().sync();
	}

	@AfterEach
	public void teardown() {
		if (client != null) {
			client.shutdown();
		}
	}

	protected String[] fileCommandArgs(String filename) throws Exception {
		try (InputStream inputStream = getClass().getResourceAsStream(filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			command = removePreamble(command);
			command = command.replace("redis://localhost:6379", redisURI(redis).toURI().toString());
			command = command.replace("--host localhost", "--host " + redis.getHost());
			command = command.replace("-h localhost", "-h " + redis.getHost());
			command = command.replace("--port 6379", "--port " + redis.getFirstMappedPort());
			command = command.replace("-p 6379", "--port " + redis.getFirstMappedPort());
			return CommandLineUtils.translateCommandline(filter(command));
		}
	}

	protected String filter(String command) {
		return command;
	}

	protected void runFile(String filename) throws Exception {
		execute(fileCommandArgs(filename));
	}

	protected RedisURI redisURI(GenericContainer redis) {
		return RedisURI.create(redis.getHost(), redis.getFirstMappedPort());
	}

	protected abstract int execute(String[] args) throws Exception;

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

}
