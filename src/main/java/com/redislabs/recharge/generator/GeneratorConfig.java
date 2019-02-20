package com.redislabs.recharge.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class GeneratorConfig {

	@Autowired
	private RechargeConfiguration config;

	public GeneratorReader reader() {
		GeneratorReader reader = new GeneratorReader();
		reader.setFields(config.getGenerator().getFields());
		reader.setLocale(config.getGenerator().getLocale());
		reader.setMapExpression(config.getGenerator().getMap());
		if (config.getFields().length == 0) {
			config.setFields(config.getGenerator().getFields().keySet().toArray(new String[0]));
		}
		return reader;
	}
}
