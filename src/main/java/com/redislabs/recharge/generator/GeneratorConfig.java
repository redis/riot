package com.redislabs.recharge.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class GeneratorConfig {

	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	public GeneratorReader reader() {
		GeneratorConfiguration generator = config.getReader().getGenerator();
		GeneratorReader reader = new GeneratorReader();
		reader.setFields(generator.getFields());
		reader.setLocale(generator.getLocale());
		reader.setMapExpression(generator.getMap());
		config.getWriter().getRedis().setCollectionFields(generator.getFields().keySet().toArray(new String[0]));
		reader.setConnection(connection);
		return reader;
	}
}
