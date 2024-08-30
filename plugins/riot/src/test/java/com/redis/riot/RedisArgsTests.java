package com.redis.riot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;

class RedisArgsTests {

	@Test
	void testSimpleRedisArgsURI() {
		SimpleRedisArgs args = new SimpleRedisArgs();
		RedisURI baseUri = RedisURI.create("redis://localhost");
		args.setUri(baseUri);
		args.setClientName("ansdf");
		RedisURI uri = args.redisURI();
		Assertions.assertEquals(baseUri.getHost(), uri.getHost());
		Assertions.assertEquals(baseUri.getPort(), uri.getPort());
		Assertions.assertEquals(args.getClientName(), uri.getClientName());
	}

	@Test
	void testRedisURIParser() {
		String host = "blah";
		int port = 123;
		RedisURI uri = Riot.parseRedisURI(host + ":" + port);
		Assertions.assertEquals(host, uri.getHost());
		Assertions.assertEquals(port, uri.getPort());
	}

}
