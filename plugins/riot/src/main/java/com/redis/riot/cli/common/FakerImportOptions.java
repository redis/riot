package com.redis.riot.cli.common;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.springframework.util.Assert;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FakerImportOptions {

	public static final int DEFAULT_START = 1;
	public static final int DEFAULT_COUNT = 1000;
	public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;
	public static final boolean DEFAULT_INCLUDE_METADATA = false;

	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int start = DEFAULT_START;
	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = DEFAULT_COUNT;
	@Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, String> fields = new LinkedHashMap<>();
	@Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
	private Optional<String> redisearchIndex = Optional.empty();
	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
	private Locale locale = DEFAULT_LOCALE;
	@Option(names = "--metadata", description = "Include metadata (index, partition).")
	private boolean includeMetadata = DEFAULT_INCLUDE_METADATA;

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

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
				+ ", includeMetadata=" + includeMetadata + ", count=" + count + "]";
	}

}
