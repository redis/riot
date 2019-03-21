package com.redislabs.recharge.generator;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class GeneratorConfig {

	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	@Bean
	@StepScope
	public GeneratorReader generatorReader() {
		GeneratorConfiguration generator = config.getGenerator();
		GeneratorReader reader = new GeneratorReader();
		reader.setFields(generator.getFields());
		reader.setLocale(generator.getLocale());
		reader.setMapExpression(generator.getMap());
		config.getRedis().setCollectionFields(generator.getFields().keySet().toArray(new String[0]));
		reader.setConnection(connection);
		return reader;
	}
}
