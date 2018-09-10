package com.redislabs.recharge.generator;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration.GeneratorReaderConfiguration;

@Component
public class GeneratorBatchConfig {

	public GeneratorEntityItemReader getReader(StringRedisConnection connection, GeneratorReaderConfiguration config) {
		Map<String, String> generatorFields = config.getFields();
		return new GeneratorEntityItemReader(connection, config.getLocale(), generatorFields);

	}

}
