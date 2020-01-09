package com.redislabs.riot.redis.writer;

public interface RedisWriter<R, O> {

	Object write(R redis, O item) throws Exception;

}
