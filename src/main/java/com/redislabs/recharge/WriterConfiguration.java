package com.redislabs.recharge;

import com.redislabs.recharge.redis.RedisSinkConfiguration;

import lombok.Data;

@Data
public class WriterConfiguration {

	private RedisSinkConfiguration redis = new RedisSinkConfiguration();

}
