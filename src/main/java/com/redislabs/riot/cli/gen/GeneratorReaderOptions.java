package com.redislabs.riot.cli.gen;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.redislabs.riot.batch.generator.GeneratorReader;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class GeneratorReaderOptions {

	@Option(names = "--faker", arity = "1..*", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = { "-d",
			"--data" }, arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields = new LinkedHashMap<>();
	@Option(names = { "-l",
			"--locale" }, description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;

	public GeneratorReader reader() {
		return new GeneratorReader().locale(locale).fakerFields(fakerFields).simpleFields(simpleFields);
	}

	public boolean isSet() {
		return !fakerFields.isEmpty() || !simpleFields.isEmpty();
	}

}
