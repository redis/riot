package com.redislabs.riot;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.PhoneticMatcher;
import com.redislabs.lettusearch.search.field.TextField;

public abstract class AbstractBaseTest {

	protected final static String FIELD_ABV = "abv";
	protected final static String FIELD_ID = "id";
	protected final static String FIELD_NAME = "name";
	protected final static String FIELD_STYLE = "style";
	protected final static String FIELD_OUNCES = "ounces";
	protected final static String INDEX = "beers";

	private RediSearchClient client;
	protected StatefulRediSearchConnection<String, String> connection;

	@Before
	public void setup() throws IOException {
		client = RediSearchClient.create("redis://localhost");
		connection = client.connect();
		RediSearchCommands<String, String> commands = connection.sync();
		commands.flushall();
		SchemaBuilder schema = Schema.builder();
		schema.field(TextField.builder().name(FIELD_NAME).sortable(true).build());
		schema.field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_ABV).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build());
		commands.create(INDEX, schema.build());
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

}
