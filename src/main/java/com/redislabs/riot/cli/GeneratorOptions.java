package com.redislabs.riot.cli;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.riot.generator.GeneratorReader;

import picocli.CommandLine.Option;

public class GeneratorOptions {

	@Option(arity = "1..*", names = "--faker-fields", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = "--simple-fields", arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields = new LinkedHashMap<>();
	@Option(names = { "-l",
			"--locale" }, description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = { "--faker-help" }, description = "Show all available Faker properties")
	private boolean fakerHelp;

	public boolean isFakerHelp() {
		return fakerHelp;
	}

	public GeneratorReader reader() {
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
		fakerFields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		return new GeneratorReader(locale, expressionMap, simpleFields);
	}

}
