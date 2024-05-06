package com.redis.riot.cli;

import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.core.AbstractImport;
import com.redis.riot.faker.FakerImport;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "faker-import", description = "Import from Faker.")
public class FakerImportCommand extends AbstractImportCommand {

	@Parameters(arity = "1..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
	private Map<String, Expression> fields;

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = FakerImport.DEFAULT_COUNT;

	@Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
	private String searchIndex;

	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
	private Locale locale = FakerImport.DEFAULT_LOCALE;

	@Override
	protected AbstractImport importCallable() {
		FakerImport callable = new FakerImport();
		callable.setFields(fields);
		callable.setCount(count);
		callable.setLocale(locale);
		callable.setSearchIndex(searchIndex);
		return callable;
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

	public String getSearchIndex() {
		return searchIndex;
	}

	public void setSearchIndex(String searchIndex) {
		this.searchIndex = searchIndex;
	}

	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

}
