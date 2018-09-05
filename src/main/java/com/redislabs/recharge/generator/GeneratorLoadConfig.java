package com.redislabs.recharge.generator;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

@Component
public class GeneratorLoadConfig {

	@Autowired
	private StringRedisTemplate redisTemplate;

	public GeneratorEntityItemReader getReader(EntityConfiguration entity) {
		Map<String, String> generatorFields = entity.getGenerator();
		if (entity.getFields() == null) {
			entity.setFields(generatorFields.keySet().toArray(new String[0]));
		}
		return new GeneratorEntityItemReader(redisTemplate, entity.getFakerLocale(), generatorFields);

	}

}
