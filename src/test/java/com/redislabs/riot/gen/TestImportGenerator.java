package com.redislabs.riot.gen;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.redislabs.riot.BaseTest;
import com.redislabs.riot.generator.GeneratorReader;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;

public class TestImportGenerator extends BaseTest {

	@Test
	public void importSimple() throws Exception {
		run("gen --batch 100 --threads 3 --max 10000 -s 100b=100 -s 1000b=1000 --keyspace simple --keys index");
		List<String> keys = connection.sync().keys("simple:*");
		Assert.assertTrue(keys.size() > 9000);
		Map<String, String> simple123 = connection.sync().hgetall("simple:123");
		Assert.assertTrue(simple123.containsKey(GeneratorReader.FIELD_THREAD));
		Assert.assertEquals("3", simple123.get(GeneratorReader.FIELD_THREADS));
		Assert.assertEquals(100, simple123.get("100b").length());
		Assert.assertEquals(1000, simple123.get("1000b").length());
	}

	@Test
	public void importFaker() throws Exception {
		run("gen --batch=50 --max=10000 -f ip=number.digits(4) -f lease=number.digits(2) -f time=number.digits(5) --command zadd --keyspace=leases --keys=ip --fields=lease --score=time");
		List<String> keys = connection.sync().keys("leases:*");
		Assert.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assert.assertTrue(connection.sync().zcard(key) > 0);
	}

	@Test
	public void importStream() throws Exception {
		run("gen --batch 50 --max 1000 -f id=index -f category=number.randomDigit --command xadd --keyspace teststream --keys category");
		List<StreamMessage<String, String>> messages = connection.sync().xrange("teststream:1", Range.unbounded());
		Assert.assertTrue(messages.size() > 0);
	}

}
