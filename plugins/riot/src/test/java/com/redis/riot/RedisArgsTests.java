package com.redis.riot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.Range;

class RedisArgsTests {

	@Test
	void redisArgsURI() {
		RedisArgs args = new RedisArgs();
		RedisURI baseUri = RedisURI.create("redis://localhost");
		args.setUri(baseUri);
		args.setClientName("ansdf");
		RedisURI uri = RedisContext.of(args.getUri(), args).getUri();
		Assertions.assertEquals(baseUri.getHost(), uri.getHost());
		Assertions.assertEquals(baseUri.getPort(), uri.getPort());
		Assertions.assertEquals(args.getClientName(), uri.getClientName());
	}

	@Test
	void parseRedisURI() {
		String host = "blah";
		int port = 123;
		RedisURI uri = new RedisURIConverter().convert(host + ":" + port);
		Assertions.assertEquals(host, uri.getHost());
		Assertions.assertEquals(port, uri.getPort());
	}

	@Test
	void parseRange() {
		Assertions.assertEquals(new Range(123, 123, false, false, null), Range.valueOf("123"));
		Assertions.assertEquals(new Range(0, 123, false, false, null), Range.valueOf("0..123"));
		Assertions.assertEquals(new Range(123, Integer.MAX_VALUE, true, false, null), Range.valueOf("123..*"));
	}

}
