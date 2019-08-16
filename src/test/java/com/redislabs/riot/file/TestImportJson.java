package com.redislabs.riot.file;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.redislabs.riot.BaseTest;

public class TestImportJson extends BaseTest {

	@Test
	public void importJson() throws Exception {
		String url = getClass().getClassLoader().getResource("es_test-index.json").getFile();
		run("file-import --file %s --keyspace estest --keys _id", url);
		Assert.assertEquals(2, connection.sync().keys("estest:*").size());
		Map<String, String> doc1 = connection.sync().hgetall("estest:doc1");
		Assert.assertEquals("ruan", doc1.get("_source.name"));
		Assert.assertEquals("1,3", doc1.get("_source.articles"));
	}

	@Test
	public void importBeersJson() throws Exception {
		run("file-import --url https://raw.githubusercontent.com/rethinkdb/beerthink/master/data/beers.json --keyspace beerjson --keys id");
		List<String> keys = connection.sync().keys("beerjson:*");
		Assert.assertEquals(4432, keys.size());
		Map<String, String> beer1 = connection.sync().hgetall("beerjson:1");
		Assert.assertEquals("Hocus Pocus", beer1.get("name"));
	}
}
