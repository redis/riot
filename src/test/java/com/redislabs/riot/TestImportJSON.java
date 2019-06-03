package com.redislabs.riot;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestImportJSON extends BaseTest {

	@Test
	public void importJSON() throws MalformedURLException, URISyntaxException {
		String url = getClass().getClassLoader().getResource("es_test-index.json").toURI().toURL().toString();
		run("import json --url " + url + " hash --keyspace estest --keys _id");
		Assert.assertEquals(2, connection.sync().keys("estest:*").size());
		Map<String, String> doc1 = connection.sync().hgetall("estest:doc1");
		Assert.assertEquals("ruan", doc1.get("_source.name"));
		Assert.assertEquals("1,3", doc1.get("_source.articles"));
	}
	
	@Test
	public void importBeersJson() {
		run("import json --url=https://raw.githubusercontent.com/rethinkdb/beerthink/master/data/beers.json hash --keyspace beerjson --keys id");
		List<String> keys = connection.sync().keys("beerjson:*");
		Assert.assertEquals(4432, keys.size());
		Map<String, String> beer1 = connection.sync().hgetall("beerjson:1");
		Assert.assertEquals("Hocus Pocus", beer1.get("name"));
	}
}
