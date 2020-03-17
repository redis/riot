package com.redislabs.riot.redis.writer;

public interface CommandWriter<O> {

	Object write(Object redis, O item) throws Exception;

}
