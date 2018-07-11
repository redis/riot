package com.redislabs.recharge.dummy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class DummyStep {

	@Autowired
	private RechargeConfiguration config;

	public DummyItemReader reader() {
		if (config.getKey().getPrefix() == null) {
			config.getKey().setPrefix("dummy");
		}
		if (config.getKey().getFields() == null || config.getKey().getFields().length == 0) {
			config.getKey().setFields(new String[] { DummyItemReader.FIELD });
		}
		return new DummyItemReader();
	}

}
