package com.redislabs.riot;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisURI;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.OS;

public class BaseTest {

	private final static String COMMAND_START = "$ riot ";

	private static RedisServer server;
	private static RediSearchClient client;
	private static StatefulRediSearchConnection<String, String> connection;
	protected static final int BEER_COUNT = 2410;

	@BeforeAll
	public static void setup() throws IOException {
		RedisExecProvider provider = RedisExecProvider.defaultProvider().override(OS.MAC_OS_X,
				"/usr/local/bin/redis-server");
		server = RedisServer.builder().redisExecProvider(provider).port(16379)
				.setting("loadmodule /Users/jruaux/git/RediSearch/build/redisearch.so").build();
		server.start();
		client = RediSearchClient.create(RedisURI.create("localhost", 16379));
		connection = client.connect();
	}

	@BeforeEach
	public void flushAll() {
		connection.sync().flushall();
	}

	protected RediSearchCommands<String, String> commands() {
		return connection.sync();
	}

	@AfterAll
	public static void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
		}
		if (server != null) {
			server.stop();
		}
	}

	public static int runFile(String filename, Object... args) throws Exception {
		return runFileWithServer(filename, "-s localhost:16379", args);
	}

	public static int runFileWithServer(String filename, String serverOptions, Object... args) throws Exception {
		try (InputStream inputStream = BaseTest.class.getResourceAsStream("/commands/" + filename + ".txt")) {
			String line = new String(inputStream.readAllBytes());
			String command = serverOptions + " " + line.substring(COMMAND_START.length());
			return new Riot().execute(CommandLineUtils.translateCommandline(String.format(command, args)));
		}
	}

}
