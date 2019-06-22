package com.redislabs.riot;

import org.junit.Assert;
import org.junit.Test;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.PhoneticMatcher;
import com.redislabs.lettusearch.search.field.TextField;

public class TestImportToRediSearch extends BaseTest {

	@Test
	public void importBeers() throws Exception {
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		String INDEX = "beerIdx";
		RediSearchCommands<String, String> commands = connection.sync();
		commands.flushall();
		SchemaBuilder schema = Schema.builder();
		schema.field(TextField.builder().name(FIELD_NAME).sortable(true).build());
		schema.field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_ABV).sortable(true).build());
		schema.field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build());
		commands.create(INDEX, schema.build());
		run("csv --header --include 1 3 4 5 6 --url https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv redisearch --index beerIdx --keys id");
		SearchResults<String, String> results = connection.sync().search(INDEX, "*");
		Assert.assertEquals(2410, results.getCount());
	}

}
