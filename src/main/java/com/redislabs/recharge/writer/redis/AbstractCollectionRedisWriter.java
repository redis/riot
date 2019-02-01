package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.CollectionRedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public abstract class AbstractCollectionRedisWriter extends AbstractPipelineRedisWriter {

	private CollectionRedisWriterConfiguration collectionConfig;

	protected AbstractCollectionRedisWriter(RediSearchClient client, RedisWriterConfiguration writer,
			CollectionRedisWriterConfiguration collectionConfig) {
		super(client, writer);
		this.collectionConfig = collectionConfig;
	}

	protected String getMemberId(Map<String, Object> record) {
		return getValues(record, collectionConfig.getFields());
	}

}
