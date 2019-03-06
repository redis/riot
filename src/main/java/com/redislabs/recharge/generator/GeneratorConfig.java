package com.redislabs.recharge.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class GeneratorConfig {

	@Autowired
	private RechargeConfiguration config;

	public GeneratorReader reader() {
		GeneratorConfiguration generator = config.getGenerator();
		GeneratorReader reader = new GeneratorReader();
		reader.setFields(generator.getFields());
		reader.setLocale(generator.getLocale());
		reader.setMapExpression(generator.getMap());
		config.getRedis().setCollectionFields(generator.getFields().keySet().toArray(new String[0]));
		return reader;
	}
}
