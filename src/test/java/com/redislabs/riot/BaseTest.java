package com.redislabs.riot;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

public class BaseTest {

	private RediSearchClient client;
	protected StatefulRediSearchConnection<String, String> connection;

	@Before
	public void setup() throws IOException {
		client = RediSearchClient.create("redis://localhost");
		connection = client.connect();
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

	protected void run(String string) throws Exception {
		RiotApplication.main(string.split(" "));
	}

}
