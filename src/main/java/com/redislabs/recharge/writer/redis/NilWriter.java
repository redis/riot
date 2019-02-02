package com.redislabs.recharge.writer.redis;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public class NilWriter extends AbstractPipelineRedisWriter {

	private NilConfiguration nil;

	public NilWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer);
		this.nil = writer.getNil();
	}

	@Override
	public void write(List<? extends Map> items) {
		if (nil == null) {
			return;
		}
		try {
			Thread.sleep(nil.getSleepInMillis());
		} catch (InterruptedException e) {
			log.error("Sleep interruped", e);
		}
	}

	@Override
	protected void write(String key, Map record) {
	}
}
