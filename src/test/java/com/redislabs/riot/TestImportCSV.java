package com.redislabs.riot;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestImportCSV extends BaseTest {

	@Test
	public void importHashes() throws Exception {
		run("import csv --url https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv --header hash --keyspace beer --keys id");
		List<String> keys = connection.sync().keys("beer:*");
		Assert.assertEquals(2410, keys.size());
	}
}
