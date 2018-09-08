package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NilWriter extends AbstractRedisWriter {

	private int currentItemCount = 0;
	private NilConfiguration nil;

	public NilWriter(StringRedisTemplate template, RedisWriterConfiguration writer) {
		super(template, writer);
		this.nil = writer.getNil();
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		if (nil.getSleepInMillis() > 0) {
			try {
				Thread.sleep(nil.getSleepInMillis());
			} catch (InterruptedException e) {
				log.error("Sleep interruped", e);
			}
		}
	}
}
