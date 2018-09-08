package com.redislabs.recharge.generator;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration.GeneratorReaderConfiguration;

@Component
public class GeneratorLoadConfig {

	@Autowired
	private StringRedisTemplate redisTemplate;

	public GeneratorEntityItemReader getReader(GeneratorReaderConfiguration config) {
		Map<String, String> generatorFields = config.getFields();
		return new GeneratorEntityItemReader(redisTemplate, config.getLocale(), generatorFields);

	}

}
