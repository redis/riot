package com.redislabs.recharge.batch;

import java.util.Map;

import org.springframework.data.redis.connection.RedisConnection;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ItemContext {

	private RedisConnection redis;
	private Map<String, Object> in;
}
