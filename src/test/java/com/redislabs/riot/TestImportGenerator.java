package com.redislabs.riot;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.redislabs.riot.generator.AbstractGeneratorReader;

public class TestImportGenerator extends BaseTest {

	@Test
	public void importSimple() throws Exception {
		run("--batch 100 --threads 3 gen --type=simple --max 10000 100b=100 1000b=1000 redis hash --keyspace simple --keys index");
		List<String> keys = connection.sync().keys("simple:*");
		Assert.assertTrue(keys.size() > 9000);
		Map<String, String> simple123 = connection.sync().hgetall("simple:123");
		Assert.assertTrue(simple123.containsKey(AbstractGeneratorReader.FIELD_THREAD));
		Assert.assertEquals("3", simple123.get(AbstractGeneratorReader.FIELD_THREADS));
		Assert.assertEquals(100, simple123.get("100b").length());
		Assert.assertEquals(1000, simple123.get("1000b").length());
	}

	@Test
	public void importFaker() throws Exception {
		run("--batch=50 gen --max=10000 ip=number.digits(4) lease=number.digits(2) time=number.digits(5) redis zset --keyspace=leases --keys=ip --fields=lease --score=time");
		List<String> keys = connection.sync().keys("leases:*");
		Assert.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assert.assertTrue(connection.sync().zcard(key) > 0);
	}

}
