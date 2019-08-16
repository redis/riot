package com.redislabs.riot;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.slf4j.impl.SimpleLogger;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import picocli.CommandLine;

public class BaseTest {

	private RediSearchClient client;
	protected StatefulRediSearchConnection<String, String> connection;

	@Before
	public void setup() throws IOException {
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "info");
		client = RediSearchClient.create("redis://localhost");
		connection = client.connect();
		connection.sync().flushall();
	}

	@After
	public void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
		}
	}

	protected void run(String template, Object... args) {
		new CommandLine(new Riot()).execute(String.format(template, args).split(" "));
	}

}
