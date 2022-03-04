package com.redis.riot.gen;

import java.util.Locale;
import java.util.Map;

import picocli.CommandLine;

public class GenerateOptions {

	@CommandLine.Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, String> fakerFields;
	@CommandLine.Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields", paramLabel = "<name>")
	private String fakerIndex;
	@CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
	private boolean includeMetadata;
	@CommandLine.Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long start = 0;
	@CommandLine.Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long end = 1000;
	@CommandLine.Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long sleep = 0;

	public Map<String, String> getFakerFields() {
		return fakerFields;
	}

	public void setFakerFields(Map<String, String> fakerFields) {
		this.fakerFields = fakerFields;
	}

	public String getFakerIndex() {
		return fakerIndex;
	}

	public void setFakerIndex(String fakerIndex) {
		this.fakerIndex = fakerIndex;
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

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

}
