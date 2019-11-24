package com.redislabs.riot.batch.redis;

public interface RedisWriter<R, O> {

	Object write(R redis, O item) throws Exception;

}
