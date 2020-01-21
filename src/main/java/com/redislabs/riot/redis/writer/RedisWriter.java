package com.redislabs.riot.redis.writer;

public interface RedisWriter<O> {

	Object write(Object redis, O item) throws Exception;

}
