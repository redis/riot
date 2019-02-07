package com.redislabs.recharge.redis.writers;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public class NilWriter extends AbstractItemStreamItemWriter<Map> {

	private NilConfiguration config;

	public NilWriter(NilConfiguration config) {
		this.config = config;
	}

	@Override
	public void write(List<? extends Map> items) {
		if (config.getSleepInMillis() > 0) {
			try {
				Thread.sleep(config.getSleepInMillis());
			} catch (InterruptedException e) {
				log.error("Sleep interruped", e);
			}
		}
	}

}
