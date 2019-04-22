package com.redislabs.riot;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class FileImportTest extends AbstractBaseTest {

	@Test
	public void testFileImport() throws Exception {
		String[] args = "import csv --url https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv --header hash --keyspace beer --keys id"
				.split(" ");
		RiotApplication.main(args);
		List<String> keys = connection.sync().keys("beer:*");
		Assert.assertEquals(2410, keys.size());
	}

}
