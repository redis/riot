package com.redislabs.riot.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

public class SimpleGeneratorReader extends GeneratorReader {

	private Map<String, Integer> fields = new LinkedHashMap<>();

	public SimpleGeneratorReader(Map<String, Integer> fields) {
		this.fields = fields;
	}

	@Override
	protected void generate(Map<String, Object> map) {
		fields.forEach((name, size) -> map.put(name, RandomStringUtils.randomAscii(size)));
	}

}
