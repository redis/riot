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

import picocli.CommandLine;

public class BaseTest {

	private final static String COMMAND_START = "$ riot ";

	private static RediSearchClient client;
	private static StatefulRediSearchConnection<String, String> connection;
	protected static final int BEER_COUNT = 2410;

	@BeforeAll
	public static void setup() throws IOException {
		client = RediSearchClient.create("redis://localhost");
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
	}

	protected void runFile(String filename, Object... args) throws Exception {
		try (InputStream inputStream = getClass().getResourceAsStream("/commands/" + filename + ".txt")) {
			runCommand(new String(inputStream.readAllBytes()), args);
		}
	}

	protected void runCommand(String line, Object... args) throws Exception {
		String command = line.startsWith(COMMAND_START) ? line.substring(COMMAND_START.length()) : line;
		new CommandLine(new Riot()).execute(CommandLineUtils.translateCommandline(String.format(command, args)));
	}

}
