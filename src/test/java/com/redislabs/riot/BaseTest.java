package com.redislabs.riot;

import org.junit.BeforeClass;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

public class BaseTest {

	@BeforeClass
	public static void flushall() {
		RedisClient.create(RedisURI.create("localhost", 6379)).connect().sync().flushall();
	}
}
