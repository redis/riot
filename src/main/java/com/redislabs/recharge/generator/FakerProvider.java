package com.redislabs.recharge.generator;

import java.util.Locale;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.javafaker.Faker;
import com.redislabs.recharge.config.Recharge;

@Component
public class FakerProvider {

	@Autowired
	private Recharge config;

	private Faker faker;

	@PostConstruct
	public void open() {
		this.faker = new Faker(new Locale(config.getGenerator().getLocale()));
	}

	public Faker getFaker() {
		return faker;
	}

	public void setFaker(Faker faker) {
		this.faker = faker;
	}

}
