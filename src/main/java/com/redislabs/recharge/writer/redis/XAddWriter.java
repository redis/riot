package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.StreamConfiguration;

import io.lettuce.core.XAddArgs;

public class XAddWriter extends AbstractSyncRedisWriter {

	private StreamConfiguration stream;

	public XAddWriter(RediSearchClient client, RedisWriterConfiguration entity) {
		super(client, entity);
		this.stream = entity.getStream();
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		commands.xadd(key, getXAddArgs(record), convert(record));
	}

	private XAddArgs getXAddArgs(Map<String, Object> record) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(stream.isApproximateTrimming());
		if (stream.getId() != null) {
			args.id(converter.convert(record.getOrDefault(stream.getId(), stream.getId()), String.class));
		}
		if (stream.getMaxlen() != null) {
			args.maxlen(stream.getMaxlen());
		}
		return args;
	}
}