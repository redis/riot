package com.redislabs.riot.generator;

import java.util.Locale;

import com.github.javafaker.Faker;

public class GeneratorFaker extends Faker {

	private FakerGeneratorReader reader;

	public GeneratorFaker(Locale locale, FakerGeneratorReader reader) {
		super(locale);
		this.reader = reader;
	}

	public long index() {
		return reader.index();
	}

	public int threads() {
		return reader.partitions();
	}

	public int thread() {
		return reader.partition();
	}

}
