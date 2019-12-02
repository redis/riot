package com.redislabs.riot.batch.redis.writer;

public interface RedisWriter<R, O> {

	Object write(R redis, O item) throws Exception;

}
