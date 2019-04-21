package com.redislabs.riot;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.RiotApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RiotApplication.class, properties = { "spring.config.location=examples/file/beer.yml" })
public class FileImportTest extends BaseTest {

	@Autowired
	StatefulRediSearchConnection<String, String> connection;

	@Test
	public void testFileImport() {
		List<String> keys = connection.sync().keys("beer:*");
		Assert.assertEquals(2410, keys.size());
	}

}
