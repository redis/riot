package com.redis.riot.gen;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.springframework.util.Assert;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FakerGeneratorOptions extends BaseGeneratorOptions {

	@Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, String> fields = new LinkedHashMap<>();
	@Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
	private Optional<String> redisearchIndex = Optional.empty();
	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--metadata", description = "Include metadata (index, partition).")
	private boolean includeMetadata;

	public Map<String, String> getFields() {
		return fields;
	}

	public void setFields(Map<String, String> fields) {
		Assert.notNull(fields, "Fields must not be null");
		this.fields = fields;
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
		Assert.notNull(locale, "Locale must not be null");
		this.locale = locale;
	}

	public boolean isIncludeMetadata() {
		return includeMetadata;
	}

	public void setIncludeMetadata(boolean includeMetadata) {
		this.includeMetadata = includeMetadata;
	}

	@Override
	public String toString() {
		return "FakerGeneratorOptions [fields=" + fields + ", redisearchIndex=" + redisearchIndex + ", locale=" + locale
				+ ", includeMetadata=" + includeMetadata + ", start=" + start + ", count=" + count + "]";
	}

}
