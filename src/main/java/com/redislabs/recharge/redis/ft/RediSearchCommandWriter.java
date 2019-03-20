package com.redislabs.recharge.redis.ft;

import com.redislabs.recharge.redis.PipelineRedisWriter;

public abstract class RediSearchCommandWriter<T extends RediSearchCommandConfiguration> extends PipelineRedisWriter {

	protected T config;

	protected RediSearchCommandWriter(T config) {
		this.config = config;
	}

}