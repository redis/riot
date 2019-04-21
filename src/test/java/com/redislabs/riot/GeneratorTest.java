package com.redislabs.riot;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.RiotApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RiotApplication.class, properties = {
		"spring.config.location=examples/generator/people.yml" })
public class GeneratorTest extends BaseTest {

	@Autowired
	StatefulRediSearchConnection<String, String> connection;

	@Test
	public void testFileImport() {
		List<String> keys = connection.sync().keys("people:*");
		Assert.assertEquals(1000, keys.size());
		Map<String, String> people1 = connection.sync().hgetall("people:1");
		Assert.assertEquals("1", people1.get("id"));
		Assert.assertTrue(people1.containsKey("firstName"));
		Assert.assertTrue(people1.containsKey("lastName"));
		Assert.assertTrue(people1.containsKey("address"));
	}

}
