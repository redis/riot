package com.redislabs.recharge;

import com.redislabs.recharge.redis.RedisSinkConfiguration;

import lombok.Data;

@Data
public class SinkConfiguration {

	private RedisSinkConfiguration redis = new RedisSinkConfiguration();

}
