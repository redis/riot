package com.redislabs.recharge.generator;

import java.util.Locale;

import org.ruaux.pojofaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.config.RechargeConfiguration;

@Configuration
public class GeneratorStep {

	@Autowired
	private RechargeConfiguration config;

	public FakeItemReader reader() {
		Faker faker = new Faker(new Locale(config.getGenerator().getLocale()));
		return new FakeItemReader(faker, config.getGenerator().getFields().entrySet());
	}

}