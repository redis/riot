package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NilWriter extends AbstractRedisWriter {

	private int currentItemCount = 0;
	private NilConfiguration nil;

	public NilWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer);
		this.nil = writer.getNil();
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		if (nil.getSleepInMillis() > 0) {
			try {
				Thread.sleep(nil.getSleepInMillis());
			} catch (InterruptedException e) {
				log.error("Sleep interruped", e);
			}
		}
	}
}
