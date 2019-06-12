package com.redislabs.riot;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;

public class TestImportCSV extends BaseTest {

	@Test
	public void importHashes() throws Exception {
		run("import csv --url https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv --header hash --keyspace beer --keys id");
		List<String> keys = connection.sync().keys("beer:*");
		Assert.assertEquals(2410, keys.size());
	}
	
	@Test
	public void importStream() throws Exception {
		run("import --batch 50 faker --max 1000 --field id=sequence --field category=number.randomDigit stream --keyspace teststream --keys category");
		List<StreamMessage<String, String>> messages = connection.sync().xrange("teststream:1", Range.unbounded());
		Assert.assertTrue(messages.size()>0);
	}
	
}
