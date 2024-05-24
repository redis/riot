package com.redis.riot;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FakerArgs {

	public static final int DEFAULT_COUNT = 1000;
	public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

	@Parameters(arity = "1..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, Expression> fields = new LinkedHashMap<>();

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = DEFAULT_COUNT;

	@Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
	private String searchIndex;

	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
	private Locale locale = DEFAULT_LOCALE;

	public String getSearchIndex() {
		return searchIndex;
	}

	public void setSearchIndex(String index) {
		this.searchIndex = index;
	}

	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public Map<String, Expression> getFields() {
		return fields;
	}

	public void setFields(Map<String, Expression> fields) {
		this.fields = fields;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
