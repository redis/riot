package com.redis.riot.gen;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import picocli.CommandLine;

public class FakerGeneratorOptions extends GeneratorOptions {

	@CommandLine.Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, String> fields;
	@CommandLine.Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields", paramLabel = "<name>")
	private Optional<String> redisearchIndex = Optional.empty();
	@CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
	private boolean includeMetadata;

	public Map<String, String> getFields() {
		return fields;
	}

	public void setFields(Map<String, String> fakerFields) {
		this.fields = fakerFields;
	}

	public Optional<String> getRedisearchIndex() {
		return redisearchIndex;
	}

	public void setFakerIndex(String fakerIndex) {
		this.redisearchIndex = Optional.of(fakerIndex);
	}

	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public boolean isIncludeMetadata() {
		return includeMetadata;
	}

	public void setIncludeMetadata(boolean includeMetadata) {
		this.includeMetadata = includeMetadata;
	}

}
