package com.redislabs.recharge.batch;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ItemContext {

	private StringRedisTemplate redis;
	private Map<String, Object> in;
}
