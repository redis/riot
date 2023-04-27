package com.redis.riot.cli.common;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.util.ObjectUtils;

import picocli.CommandLine.Option;

public class MapProcessorOptions {

	@Option(arity = "1..*", names = "--process", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	private Map<String, Expression> spelFields;
	@Option(arity = "1..*", names = "--var", description = "Register a variable in the SpEL processor context.", paramLabel = "<v=exp>")
	private Map<String, Expression> variables;
	@Option(names = "--date", description = "Processor date format (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();
	@Option(arity = "1..*", names = "--filter", description = "Discard records using SpEL boolean expressions.", paramLabel = "<exp>")
	private String[] filters;
	@Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex.", paramLabel = "<f=exp>")
	private Map<String, String> regexes;

	public Map<String, Expression> getSpelFields() {
		return spelFields;
	}

	public void setSpelFields(Map<String, Expression> spelFields) {
		this.spelFields = spelFields;
	}

	public Map<String, Expression> getVariables() {
		return variables;
	}

	public void setVariables(Map<String, Expression> variables) {
		this.variables = variables;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String[] getFilters() {
		return filters;
	}

	public void setFilters(String[] filters) {
		this.filters = filters;
	}

	public Map<String, String> getRegexes() {
		return regexes;
	}

	public void setRegexes(Map<String, String> regexes) {
		this.regexes = regexes;
	}

	@Override
	public String toString() {
		return "MapProcessorOptions [spelFields=" + spelFields + ", variables=" + variables + ", dateFormat="
				+ dateFormat + ", filters=" + Arrays.toString(filters) + ", regexes=" + regexes + "]";
	}

	public boolean hasRegexes() {
		return !ObjectUtils.isEmpty(regexes);
	}

	public boolean hasFilters() {
		return !ObjectUtils.isEmpty(filters);
	}

	public boolean hasVariables() {
		return !ObjectUtils.isEmpty(variables);
	}

	public boolean hasSpelFields() {
		return !ObjectUtils.isEmpty(spelFields);
	}

}
