package com.redislabs.riot;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;

public class TestImportCSV extends BaseTest {

	@Test
	public void importHashes() throws Exception {
		run("file https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv --header redis hash --keyspace beer --keys id");
		List<String> keys = connection.sync().keys("beer:*");
		Assert.assertEquals(2410, keys.size());
	}

	@Test
	public void importStream() throws Exception {
		run("--batch 50 gen --type=faker --max 1000 id=index category=number.randomDigit redis stream --keyspace teststream --keys category");
		List<StreamMessage<String, String>> messages = connection.sync().xrange("teststream:1", Range.unbounded());
		Assert.assertTrue(messages.size() > 0);
	}

}
