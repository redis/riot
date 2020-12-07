package com.redislabs.riot;

import io.lettuce.core.AbstractRedisClient;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TransferContext {

	private RedisOptions redisOptions;
	private AbstractRedisClient client;

}
